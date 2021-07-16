package com.slack.kaldb.chunk;

import static com.slack.kaldb.util.ArgValidationUtils.ensureNonNullString;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.logstore.LogStore;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.logstore.search.SearchResultAggregator;
import com.slack.kaldb.logstore.search.SearchResultAggregatorImpl;
import com.spotify.futures.CompletableFutures;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The log data in each Kaldb indexer is stored as chunks. Each chunk is backed by a single instance
 * of log store. Each log store instance which internally contains a lucene index. So, a chunk is
 * identical to a shard.
 *
 * <p>Chunk manager provides a unified api to write and query all the chunks in the application. The
 * addMessage api is used by writers where as the query API is used by the readers.
 *
 * <p>Internally the chunk manager maintains a list of chunks. All chunks except one is considered
 * active. The chunk manager writes the message to the currently active chunk. Once a chunk reaches
 * a roll over point(defined by a roll over strategy), the current chunk is marked as read only. At
 * that point a new chunk is created which becomes the active chunk.
 */
public class ChunkManager<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ChunkManager.class);
  public static final long DEFAULT_ROLLOVER_FUTURE_TIMEOUT_MS = 30000;

  private final File dataDirectory;

  // TODO: ChunkDataPrefix can be moved to KaldbConfig?
  private final String chunkDataPrefix;

  private final Map<String, Chunk<T>> chunkMap = new ConcurrentHashMap<>(16);
  private final Object chunkMapSync = new Object();

  // TODO: Pass a reference to BlobFS instead of S3BlobFS.
  private final S3BlobFs s3BlobFs;
  private final String s3Bucket;
  private final ChunkRollOverStrategy chunkRollOverStrategy;
  private Chunk<T> activeChunk;

  private final MeterRegistry meterRegistry;
  private final AtomicLong liveMessagesIndexedGauge;
  private final AtomicLong liveBytesIndexedGauge;

  public static final String LIVE_MESSAGES_INDEXED = "live_messages_indexed";
  public static final String LIVE_BYTES_INDEXED = "live_bytes_indexed";

  // fields related to roll over
  private final ListeningExecutorService rolloverExecutorService;
  private final long rolloverFutureTimeoutMs;
  private ListenableFuture<Boolean> rolloverFuture;

  // TODO: We want to move this to the config eventually
  public static final int QUERY_TIMEOUT_SECONDS = 30;
  public static final int LOCAL_QUERY_THREAD_POOL_SIZE = 4;

  private static final ExecutorService queryExecutorService = queryThreadPool();

  /**
   * A flag to indicate that ingestion should be stopped. Currently, we only stop ingestion when a
   * chunk roll over fails. Ideally, access to this field should be synchronized. But we don't
   * synchronize on this field for 2 reasons: (a) This field is checked for every message so it's in
   * hot path. (b) This field is only set once in the same thread and then entire VM stops
   * afterwards.
   */
  private boolean stopIngestion;

  /**
   * For capacity planning, we want to control how many roll overs are in progress at the same time.
   * To maintain flexibility on the final policy, we will implement it using a ThreadPoolExecutor.
   * For now, we only want 1 roll over in process at any time and abort the indexer if a second roll
   * over is in process, since otherwise we will never be able to keep up. So, we create a custom
   * roll over executor that can only execute one roll over task at a time and throws a
   * RejectedExecutionHandler exception when a second one is called (the default policy).
   */
  public static ListeningExecutorService makeDefaultRollOverExecutor() {
    // TODO: Create a named thread pool and pass it in.
    ThreadPoolExecutor rollOverExecutor =
        new ThreadPoolExecutor(
            1, 1, 0, MILLISECONDS, new SynchronousQueue<>(), new ThreadPoolExecutor.AbortPolicy());
    return MoreExecutors.listeningDecorator(
        MoreExecutors.getExitingExecutorService(rollOverExecutor));
  }

  // TODO: Pass in BlobFs object instead of S3BlobFS to make this more generic.
  public ChunkManager(
      String chunkDataPrefix,
      String dataDirectory,
      ChunkRollOverStrategy chunkRollOverStrategy,
      MeterRegistry registry,
      S3BlobFs s3BlobFs,
      String s3Bucket,
      ListeningExecutorService rollOverExecutorService,
      long rollOverFutureTimeoutMs) {

    ensureNonNullString(dataDirectory, "The data directory shouldn't be empty");
    this.dataDirectory = new File(dataDirectory);
    this.chunkDataPrefix = chunkDataPrefix;
    this.chunkRollOverStrategy = chunkRollOverStrategy;
    this.meterRegistry = registry;

    // TODO: Pass in id of index in LuceneIndexStore to track this info.
    liveMessagesIndexedGauge = registry.gauge(LIVE_MESSAGES_INDEXED, new AtomicLong(0));
    liveBytesIndexedGauge = registry.gauge(LIVE_BYTES_INDEXED, new AtomicLong(0));

    this.s3BlobFs = s3BlobFs;
    this.s3Bucket = s3Bucket;
    this.rolloverExecutorService = rollOverExecutorService;
    this.rolloverFuture = null;
    this.rolloverFutureTimeoutMs = rollOverFutureTimeoutMs;
    stopIngestion = false;
    activeChunk = null;

    LOG.info(
        "Created a chunk manager with prefix {} and dataDirectory {}",
        chunkDataPrefix,
        dataDirectory);
  }

  /*
     One day we will have to think about rate limiting/backpressure and we will revisit this so it could potentially reject threads if the pool is full
  */
  private static ExecutorService queryThreadPool() {
    return Executors.newFixedThreadPool(LOCAL_QUERY_THREAD_POOL_SIZE);
  }

  /**
   * This function ingests a message into a chunk in the chunk manager. It performs the following
   * steps: 1. Find an active chunk. 2. Ingest the message into the active chunk. 3. Calls the
   * shouldRollOver function to check if the chunk is full. 4. If the chunk is full, initiate the
   * roll over of the active chunk.
   *
   * <p>We assume that there is a single chunk manager per process and only one thread is writing to
   * this class. We allow several readers though.
   *
   * @param message Message to be ingested
   * @param msgSize Serialized size of raw message in bytes.
   * @param offset Kafka offset of the message.
   *     <p>TODO: Indexer should stop cleanly if the roll over fails or an exception.
   *     <p>TODO: Delete the snapshot from local disk once it is replicated elsewhere after X min.
   */
  public void addMessage(final T message, long msgSize, long offset) throws IOException {
    if (stopIngestion) {
      // Currently, this flag is set on only a chunkRollOverException.
      LOG.warn("Stopping ingestion due to a chunk roll over exception.");
      throw new ChunkRollOverException("Stopping ingestion due to chunk roll over exception.");
    }

    // find the active chunk and add a message to it
    Chunk<T> currentChunk = getOrCreateActiveChunk();
    currentChunk.addMessage(message);
    long currentIndexedMessages = liveMessagesIndexedGauge.incrementAndGet();
    long currentIndexedBytes = liveBytesIndexedGauge.addAndGet(msgSize);

    // If active chunk is full roll it over.
    if (chunkRollOverStrategy.shouldRollOver(currentIndexedBytes, currentIndexedMessages)) {
      LOG.info(
          "After {} messages and {} bytes rolling over chunk {}.",
          currentIndexedMessages,
          currentIndexedBytes,
          currentChunk.id());
      currentChunk.info().setNumDocs(currentIndexedMessages);
      currentChunk.info().setChunkSize(currentIndexedBytes);
      doRollover(currentChunk);
    }
  }

  /**
   * This method initiates a roll over of the active chunk. In future, consider moving the some of
   * the roll over logic into ChunkImpl.
   */
  private void doRollover(Chunk<T> currentChunk) {
    // Set activeChunk to null first, so we can initiate the roll over.
    activeChunk = null;
    liveBytesIndexedGauge.set(0);
    liveMessagesIndexedGauge.set(0);
    // Set the end time of the chunk and start the roll over.
    currentChunk.info().setChunkLastUpdatedTimeSecsEpochSecs(Instant.now().getEpochSecond());

    RollOverChunkTask<T> rollOverChunkTask =
        new RollOverChunkTask<>(
            currentChunk, meterRegistry, s3BlobFs, s3Bucket, currentChunk.info().chunkId);

    if ((rolloverFuture == null) || rolloverFuture.isDone()) {
      rolloverFuture = rolloverExecutorService.submit(rollOverChunkTask);
      Futures.addCallback(
          rolloverFuture,
          new FutureCallback<>() {
            @Override
            public void onSuccess(Boolean success) {
              if (success == null || !success) {
                stopIngestion = true;
              }
            }

            @Override
            public void onFailure(Throwable t) {
              stopIngestion = true;
            }
          },
          MoreExecutors.directExecutor());
    } else {
      throw new ChunkRollOverInProgressException(
          String.format(
              "The chunk roll over %s is already in progress."
                  + "It is not recommended to index faster than we can roll over, since we may not be able to keep up.",
              currentChunk.info()));
    }
  }

  /*
   * When the ChunkManager is being closed, roll over active chunk so we can be sure that it's data is persisted in
   * a remote store.
   */
  public void rollOverActiveChunk() {
    LOG.info("Rolling over active chunk");
    doRollover(getActiveChunk());
  }

  @VisibleForTesting
  public Chunk<T> getActiveChunk() {
    return activeChunk;
  }

  /**
   * getChunk returns the active chunk. If no chunk is active because of roll over or this is the
   * first message, create one chunk and set is as active.
   *
   * <p>NOTE: Currently, this logic assumes that we are indexing live data. So, the startTime of the
   * data in the chunk is set as system time. However, this assumption may not be true always. In
   * future, set the start time of the chunk based on the timestamp from the message.
   */
  private Chunk<T> getOrCreateActiveChunk() throws IOException {
    if (activeChunk == null) {
      // TODO: Move this line into sync block if make chunk is fast.
      @SuppressWarnings("unchecked")
      LogStore<T> logStore =
          (LogStore<T>) LuceneIndexStoreImpl.makeLogStore(dataDirectory, meterRegistry);
      Chunk<T> newChunk = new ReadWriteChunkImpl<>(logStore, chunkDataPrefix, meterRegistry);
      synchronized (chunkMapSync) {
        chunkMap.put(newChunk.id(), newChunk);
        activeChunk = newChunk;
      }
    }
    return activeChunk;
  }

  /*
   * Query the chunks in the time range, aggregate the results per aggregation policy and return the results.
   * We aggregate locally and and then the query aggregator will aggregate again. This is OKAY for the current use-case we support
   * 1. topK results sorted by timestamp
   * 2. histogram over a fixed time range
   * We will not aggregate locally for future use-cases that have complex group by etc
   */
  public CompletableFuture<SearchResult<T>> query(SearchQuery query) {

    SearchResult<T> errorResult =
        new SearchResult<>(new ArrayList<>(), 0, 0, new ArrayList<>(), 0, 0, 1, 0);

    List<CompletableFuture<SearchResult<T>>> queries =
        chunkMap
            .values()
            .stream()
            .filter(
                chunk ->
                    chunk.containsDataInTimeRange(
                        query.startTimeEpochMs / 1000, query.endTimeEpochMs / 1000))
            .map(
                (chunk) ->
                    CompletableFuture.supplyAsync(() -> chunk.query(query), queryExecutorService)
                        // TODO: this will not cancel lucene query. Use ExitableDirectoryReader in
                        // the future and pass this timeout
                        .completeOnTimeout(errorResult, QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS))
            .map(
                chunkFuture ->
                    chunkFuture.exceptionally(
                        err -> {
                          // We catch IllegalArgumentException ( and any other exception that
                          // represents a parse failure ) and instead of returning an empty result
                          // we throw back an error to the user
                          if (err.getCause() instanceof IllegalArgumentException) {
                            throw (IllegalArgumentException) err.getCause();
                          }
                          return errorResult;
                        }))
            .collect(Collectors.<CompletableFuture<SearchResult<T>>>toList());

    // TODO: if all fails return error instead of empty and add test

    // Using the spotify library ( this method is much easier to operate then using
    // CompletableFuture.allOf and converting the CompletableFuture<Void> to
    // CompletableFuture<List<SearchResult>>
    CompletableFuture<List<SearchResult<T>>> searchResults = CompletableFutures.allAsList(queries);

    //noinspection unchecked
    return ((SearchResultAggregator<T>) new SearchResultAggregatorImpl<>(query))
        .aggregate(searchResults);
  }

  /**
   * Close the chunk manager safely by finishing all the pending roll overs and closing chunks
   * cleanly. To ensure data integrity don't throw exceptions before chunk close.
   *
   * <p>TODO: When closing a ChunkManager we need to ensure that all the active chunks are closed,
   * and the data is uploaded safely to a remote store. If the active chunks are not closed
   * correctly, we would throw away indexed data and would need to index the same data again.
   *
   * <p>TODO: Consider implementing async close. Also, stop new writes once close is called.
   */
  public void close() {
    LOG.info("Closing chunk manager.");

    // Stop executor service from taking on new tasks.
    rolloverExecutorService.shutdown();

    // Finish existing rollovers.
    if (rolloverFuture != null && !rolloverFuture.isDone()) {
      try {
        LOG.info("Waiting for roll over to complete before closing..");
        rolloverFuture.get(rolloverFutureTimeoutMs, MILLISECONDS);
        LOG.info("Roll over completed successfully. Closing rollover task.");
      } catch (Exception e) {
        LOG.warn("Roll over failed with Exception", e);
        // TODO: Throw a roll over failed exception and stop the indexer.
      }
    } else {
      LOG.info("Roll over future completed successfully.");
    }

    // Close roll over executor service.
    try {
      // A short timeout here is fine here since there are no more tasks.
      rolloverExecutorService.awaitTermination(1, TimeUnit.SECONDS);
      rolloverExecutorService.shutdownNow();
    } catch (InterruptedException e) {
      LOG.warn("Encountered error shutting down roll over executor.", e);
    }
    for (Chunk<T> chunk : chunkMap.values()) {
      chunk.close();
    }
    LOG.info("Closed chunk manager.");
  }

  public void removeStaleChunks(List<Map.Entry<String, Chunk<T>>> staleChunks) {
    if (staleChunks.isEmpty()) return;

    LOG.info("Stale chunks to be removed are: {}", staleChunks);

    if (chunkMap.isEmpty()) {
      LOG.warn("Possible race condition, there are no chunks in chunkMap");
    }

    staleChunks.forEach(
        entry -> {
          try {
            if (chunkMap.containsKey(entry.getKey())) {
              final Chunk<T> chunk = entry.getValue();
              String chunkInfo = chunk.info().toString();
              LOG.info("Deleting chunk {}.", chunkInfo);

              // Remove the chunk first from the map so we don't search it anymore.
              synchronized (chunkMapSync) {
                chunkMap.remove(entry.getKey());
              }

              chunk.close();
              chunk.cleanup();
              LOG.info("Deleted and cleaned up chunk {}.", chunkInfo);
            } else {
              LOG.warn(
                  "Possible bug or race condition! Chunk {} doesn't exist in chunk map {}.",
                  entry,
                  chunkMap);
            }
          } catch (Exception e) {
            LOG.warn("Exception when deleting chunk", e);
          }
        });
  }

  @VisibleForTesting
  public Map<String, Chunk<T>> getChunkMap() {
    return chunkMap;
  }

  @VisibleForTesting
  public ListenableFuture<?> getRolloverFuture() {
    return rolloverFuture;
  }
}
