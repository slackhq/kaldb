package com.slack.kaldb.logstore;

import com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.util.RuntimeHalterImpl;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * LuceneIndexStore stores a log message in a lucene index. It uses LuceneIndexWriter to create an index. The config
 * defines the behavior of the index writer. The DocumentBuilder will decide how the document is analyzed before it is
 * stored in the index.
 *
 * TODO: Each index store has a unique id that is used to as a suffix/prefix in files associated with this store?
 */
public class LuceneIndexStoreImpl implements LogStore<LogMessage> {

  private final String id = UUID.randomUUID().toString();

  private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexStoreImpl.class);
  public static final String MESSAGES_RECEIVED_COUNTER = "messages_received";
  public static final String MESSAGES_FAILED_COUNTER = "messages_failed";
  public static final String COMMITS_TIMER = "kaldb_index_commits";
  public static final String REFRESHES_TIMER = "kaldb_index_refreshes";

  public static final String FINAL_MERGES_TIMER = "kaldb_index_final_merges";

  private final SearcherManager searcherManager;
  private final DocumentBuilder<LogMessage> documentBuilder;
  private final FSDirectory indexDirectory;
  private final Timer timer;
  private final SnapshotDeletionPolicy snapshotDeletionPolicy;
  private Optional<IndexWriter> indexWriter;

  // Stats counters.
  private final Counter messagesReceivedCounter;
  private final Counter messagesFailedCounter;
  private final io.micrometer.core.instrument.Timer commitsTimer;
  private final io.micrometer.core.instrument.Timer refreshesTimer;

  private final io.micrometer.core.instrument.Timer finalMergesTimer;

  // We think if the segments being flushed to disk are smaller than this then we should use
  // compound files or not.
  // If we ever revisit this - the value was picked thinking it's a good "default"
  private final Integer CFS_FILES_SIZE_MB_CUTOFF = 128;

  private final ReentrantLock indexWriterLock = new ReentrantLock();

  // We use a phaser to allow all originally submitted writes (potentially in other threads) to
  // finish before invoking a close on the indexWriter
  private final Phaser pendingWritesPhaser = new Phaser();

  // TODO: Set the policy via a lucene config file.
  public static LuceneIndexStoreImpl makeLogStore(
      File dataDirectory, KaldbConfigs.LuceneConfig luceneConfig, MeterRegistry metricsRegistry)
      throws IOException {
    return makeLogStore(
        dataDirectory,
        LuceneIndexStoreConfig.getCommitDuration(luceneConfig.getCommitDurationSecs()),
        LuceneIndexStoreConfig.getRefreshDuration(luceneConfig.getRefreshDurationSecs()),
        luceneConfig.getEnableFullTextSearch(),
        SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_VALUE_AND_DUPLICATE_FIELD,
        metricsRegistry);
  }

  public static LuceneIndexStoreImpl makeLogStore(
      File dataDirectory,
      Duration commitInterval,
      Duration refreshInterval,
      boolean enableFullTextSearch,
      SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy fieldConflictPolicy,
      MeterRegistry metricsRegistry)
      throws IOException {
    // TODO: Move all these config values into chunk?
    // TODO: Chunk should create log store?
    LuceneIndexStoreConfig indexStoreCfg =
        new LuceneIndexStoreConfig(
            commitInterval, refreshInterval, dataDirectory.getAbsolutePath(), false);

    return new LuceneIndexStoreImpl(
        indexStoreCfg,
        SchemaAwareLogDocumentBuilderImpl.build(
            fieldConflictPolicy, enableFullTextSearch, metricsRegistry),
        metricsRegistry);
  }

  public LuceneIndexStoreImpl(
      LuceneIndexStoreConfig config,
      DocumentBuilder<LogMessage> documentBuilder,
      MeterRegistry registry)
      throws IOException {

    this.documentBuilder = documentBuilder;

    Analyzer analyzer = new StandardAnalyzer();
    this.snapshotDeletionPolicy =
        new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    IndexWriterConfig indexWriterConfig =
        buildIndexWriterConfig(analyzer, this.snapshotDeletionPolicy, config, registry);
    indexDirectory = new MMapDirectory(config.indexFolder(id).toPath());
    indexWriter = Optional.of(new IndexWriter(indexDirectory, indexWriterConfig));
    this.searcherManager = new SearcherManager(indexWriter.get(), false, false, null);

    timer = new Timer(true);
    timer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            commit();
          }
        },
        config.commitDuration.toMillis(),
        config.commitDuration.toMillis());
    timer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            refresh();
          }
        },
        config.refreshDuration.toMillis(),
        config.refreshDuration.toMillis());

    // Initialize stats counters
    messagesReceivedCounter = registry.counter(MESSAGES_RECEIVED_COUNTER);
    messagesFailedCounter = registry.counter(MESSAGES_FAILED_COUNTER);
    commitsTimer = registry.timer(COMMITS_TIMER);
    refreshesTimer = registry.timer(REFRESHES_TIMER);
    finalMergesTimer = registry.timer(FINAL_MERGES_TIMER);

    LOG.debug(
        "Created a lucene index {} at: {}", id, indexDirectory.getDirectory().toAbsolutePath());
  }

  /**
   * Attempts to determine an optimal ram buffer size based on the size of the heap. The target of
   * 10% matches that of the defaults of ES.
   *
   * @see {https://www.elastic.co/guide/en/elasticsearch/reference/current/indexing-buffer.html}
   */
  protected static long getRAMBufferSizeMB(long heapMaxBytes) {
    long targetBufferSize = 256;
    if (heapMaxBytes != Long.MAX_VALUE) {
      targetBufferSize = Math.min(2048, Math.round(heapMaxBytes / 1e6 * 0.10));
    }
    LOG.info(
        "Setting max ram buffer size to {}mb, heap max bytes detected as {}",
        targetBufferSize,
        heapMaxBytes);
    return targetBufferSize;
  }

  private IndexWriterConfig buildIndexWriterConfig(
      Analyzer analyzer,
      SnapshotDeletionPolicy snapshotDeletionPolicy,
      LuceneIndexStoreConfig config,
      MeterRegistry metricsRegistry) {
    long ramBufferSizeMb = getRAMBufferSizeMB(Runtime.getRuntime().maxMemory());
    boolean useCFSFiles = ramBufferSizeMb <= CFS_FILES_SIZE_MB_CUTOFF;
    final IndexWriterConfig indexWriterCfg =
        new IndexWriterConfig(analyzer)
            .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
            .setMergeScheduler(new KalDBMergeScheduler(metricsRegistry))
            .setRAMBufferSizeMB(ramBufferSizeMb)
            .setUseCompoundFile(useCFSFiles)
            // we sort by timestamp descending, as that is the order we expect to return results the
            // majority of the time
            .setIndexSort(
                new Sort(
                    new SortField(
                        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                        SortField.Type.LONG,
                        true)))
            .setIndexDeletionPolicy(snapshotDeletionPolicy);

    // This applies to segments when they are being merged
    // Use the default in case the ramBufferSize is below the cutoff
    if (!useCFSFiles) {
      indexWriterCfg.getMergePolicy().setNoCFSRatio(0.0);
    }

    if (config.enableTracing) {
      indexWriterCfg.setInfoStream(System.out);
    }

    return indexWriterCfg;
  }

  // TODO: IOException can be logged and recovered from?.
  private void syncCommit() throws IOException {
    indexWriterLock.lock();
    try {
      if (indexWriter.isPresent()) {
        indexWriter.get().commit();
      }
    } finally {
      indexWriterLock.unlock();
    }
  }

  private void syncRefresh() throws IOException {
    indexWriterLock.lock();
    try {
      if (indexWriter.isPresent()) {
        searcherManager.maybeRefresh();
      }
    } finally {
      indexWriterLock.unlock();
    }
  }

  private void syncFinalMerge() throws IOException {
    indexWriterLock.lock();
    try {
      if (indexWriter.isPresent()) {
        indexWriter.get().forceMerge(1);
      }
    } finally {
      indexWriterLock.unlock();
    }
  }

  @Override
  public FSDirectory getDirectory() {
    return indexDirectory;
  }

  private void handleNonFatal(Throwable ex) {
    messagesFailedCounter.increment();
    LOG.error(String.format("Exception %s processing", ex));
  }

  @Override
  public void addMessage(LogMessage message) {
    pendingWritesPhaser.register();
    try {
      messagesReceivedCounter.increment();
      if (indexWriter.isPresent()) {
        indexWriter.get().addDocument(documentBuilder.fromMessage(message));
      } else {
        LOG.error("IndexWriter should never be null when adding a message");
        throw new IllegalStateException("IndexWriter should never be null when adding a message");
      }
    } catch (FieldDefMismatchException | IllegalArgumentException e) {
      LOG.error(String.format("Indexing message %s failed with error:", message), e);
      messagesFailedCounter.increment();
    } catch (IOException e) {
      // TODO: In future may need to handle this case more gracefully.
      LOG.error("failed to add document", e);
      new RuntimeHalterImpl().handleFatal(e);
    } finally {
      pendingWritesPhaser.arriveAndDeregister();
    }
  }

  @Override
  public void commit() {
    commitsTimer.record(
        () -> {
          LOG.debug("Indexer starting commit for: " + indexDirectory.getDirectory().toString());
          try {
            syncCommit();
            LOG.debug("Indexer finished commit for: " + indexDirectory.getDirectory().toString());
          } catch (IOException e) {
            handleNonFatal(e);
          }
        });
  }

  @Override
  public void refresh() {
    refreshesTimer.record(
        () -> {
          LOG.debug("Indexer starting refresh for: " + indexDirectory.getDirectory().toString());
          try {
            syncRefresh();
            LOG.debug("Indexer finished refresh for: " + indexDirectory.getDirectory().toString());
          } catch (IOException e) {
            handleNonFatal(e);
          }
        });
  }

  @Override
  public void finalMerge() {
    finalMergesTimer.record(
        () -> {
          LOG.debug(
              "Indexer starting final merge for: " + indexDirectory.getDirectory().toString());
          try {
            syncFinalMerge();
            LOG.debug(
                "Indexer finished final merge for: " + indexDirectory.getDirectory().toString());
          } catch (IOException e) {
            handleNonFatal(e);
          }
        });
  }

  @Override
  public boolean isOpen() {
    return indexWriter.isPresent();
  }

  @Override
  public String toString() {
    return "LuceneIndexStoreImpl{"
        + "id='"
        + id
        + '\''
        + ", at="
        + getDirectory().getDirectory().toAbsolutePath()
        + '}';
  }

  @Override
  public IndexCommit getIndexCommit() {
    try {
      return snapshotDeletionPolicy.snapshot();
    } catch (IOException e) {
      LOG.error("Tried to snapshot index commit but failed", e);
    }
    return null;
  }

  @Override
  public IndexWriter getIndexWriter() {
    return indexWriter.get();
  }

  @Override
  public void releaseIndexCommit(IndexCommit indexCommit) {
    if (indexCommit != null) {
      try {
        snapshotDeletionPolicy.release(indexCommit);
      } catch (IOException e) {
        LOG.warn("Tried to release snapshot index commit but failed", e);
      }
    }
  }

  /**
   * This method closes the log store cleanly and cancels any ongoing tasks. This function cancels
   * the existing timer but doesn't run a commit or refresh. The users of this class are need to
   * ensure that the data is already committed before close.
   */
  @Override
  public void close() {
    try {
      // Wait for all previously submitted pendingWrites to finish
      pendingWritesPhaser.awaitAdvanceInterruptibly(0, 5, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.warn("Timeout waiting for pendingWritesPhaser to advance", e);
    }

    indexWriterLock.lock();
    try {
      if (indexWriter.isEmpty()) {
        // Closable.close() requires this be idempotent, so silently exit instead of throwing an
        // exception
        return;
      }

      timer.cancel();
      try {
        indexWriter.get().close();
      } catch (IllegalStateException | IOException | NoSuchElementException e) {
        LOG.error("Error closing index " + id, e);
      }
      indexWriter = Optional.empty();
    } finally {
      indexWriterLock.unlock();
    }
  }

  // TODO: Currently, deleting the index. May need to delete the folder.
  @Override
  public void cleanup() throws IOException {
    if (indexWriter.isPresent()) {
      throw new IllegalStateException("IndexWriter should be closed before cleanup");
    }
    LOG.debug("Deleting directory: {}", indexDirectory.getDirectory().toAbsolutePath());
    FileUtils.deleteDirectory(indexDirectory.getDirectory().toFile());
  }

  @Override
  public SearcherManager getSearcherManager() {
    return searcherManager;
  }

  public String getId() {
    return id;
  }

  @Override
  public ConcurrentHashMap<String, LuceneFieldDef> getSchema() {
    return documentBuilder.getSchema();
  }
}
