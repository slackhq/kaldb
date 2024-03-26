package com.slack.kaldb.server;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.slack.kaldb.chunkManager.ChunkRollOverException;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.util.RuntimeHalterImpl;
import com.slack.kaldb.writer.LogMessageWriterImpl;
import com.slack.kaldb.writer.kafka.KaldbKafkaConsumer;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KaldbIndexer creates an indexer to index the log data. The indexer also exposes a search api to
 * search the log data.
 */
public class KaldbIndexer extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbIndexer.class);

  private final AsyncCuratorFramework curatorFramework;
  private final MeterRegistry meterRegistry;
  private final KaldbConfigs.IndexerConfig indexerConfig;
  private final KaldbConfigs.KafkaConfig kafkaConfig;
  private final KaldbKafkaConsumer kafkaConsumer;
  private final IndexingChunkManager<LogMessage> chunkManager;

  /**
   * This class contains the code to needed to run a single instance of an Kaldb indexer. A single
   * instance of Kaldb indexer, indexes data from kafka into the chunk manager and provides an API
   * to search that data.
   *
   * <p>In addition, this class also contains the code to gracefully start and shutdown the server.
   *
   * <p>The only way we can ensure durability of data is when the data _and_ metadata are stored
   * reliably. So, on a indexer shutdown we need to ensure that as much of indexed data and metadata
   * is stored reliably. Otherwise, on an indexer shutdown we would end up re-indexing the data
   * which would result in wasted work.
   *
   * <p>On an indexer restart, we should start indexing at a last known good offset for that
   * partition. The indexer pre-start job, ensures that we choose the correct start offset and end
   * offset for the indexer. Optionally, the pre start task also creates an recovery task if needed,
   * since the indexer may not be able to catch up.
   */
  public KaldbIndexer(
      IndexingChunkManager<LogMessage> chunkManager,
      AsyncCuratorFramework curatorFramework,
      KaldbConfigs.IndexerConfig indexerConfig,
      KaldbConfigs.KafkaConfig kafkaConfig,
      MeterRegistry meterRegistry) {
    checkNotNull(chunkManager, "Chunk manager can't be null");
    this.curatorFramework = curatorFramework;
    this.indexerConfig = indexerConfig;
    this.kafkaConfig = kafkaConfig;
    this.meterRegistry = meterRegistry;

    // Create a chunk manager
    this.chunkManager = chunkManager;
    // set up indexing pipelne
    LogMessageWriterImpl logMessageWriterImpl = new LogMessageWriterImpl(chunkManager);
    this.kafkaConsumer = new KaldbKafkaConsumer(kafkaConfig, logMessageWriterImpl, meterRegistry);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Kaldb indexer.");
    long startOffset = indexerPreStart();
    // ensure the chunk manager is available to receive messages
    chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

    // Set the Kafka offset and pre consumer for consumption.
    kafkaConsumer.prepConsumerForConsumption(startOffset);

    LOG.info("Started Kaldb indexer.");
  }

  /**
   * Indexer pre-start cleans up stale indexer tasks and optionally creates a recovery task if we
   * can't catch up.
   */
  private long indexerPreStart() throws Exception {
    LOG.info("Starting Kaldb indexer pre start.");
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, meterRegistry);
    RecoveryTaskMetadataStore recoveryTaskMetadataStore =
        new RecoveryTaskMetadataStore(curatorFramework, true, meterRegistry);

    String partitionId = kafkaConfig.getKafkaTopicPartition();
    long maxOffsetDelay = indexerConfig.getMaxOffsetDelayMessages();

    // TODO: Move this to it's own config var.
    final long maxMessagesPerRecoveryTask = indexerConfig.getMaxMessagesPerChunk();
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskMetadataStore,
            partitionId,
            maxOffsetDelay,
            maxMessagesPerRecoveryTask,
            meterRegistry);

    long currentEndOffsetForPartition = kafkaConsumer.getEndOffSetForPartition();
    long currentBeginningOffsetForPartition = kafkaConsumer.getBeginningOffsetForPartition();
    long startOffset =
        recoveryTaskCreator.determineStartingOffset(
            currentEndOffsetForPartition, currentBeginningOffsetForPartition, indexerConfig);

    // Close these stores since we don't need them after preStart.
    snapshotMetadataStore.close();
    recoveryTaskMetadataStore.close();
    LOG.info(
        "Completed Kaldb indexer pre start - currentEndOffsetForPartition {}, currentBeginningOffsetForPartition {}, startOffset {}",
        currentEndOffsetForPartition,
        currentBeginningOffsetForPartition,
        startOffset);

    return startOffset;
  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      try {
        kafkaConsumer.consumeMessages();
      } catch (ChunkRollOverException | IOException e) {
        // Once we hit these exceptions, we likely have an issue related to storage. So, terminate
        // the program, since consuming more messages from Kafka would only make the issue worse.
        LOG.error("FATAL: Encountered an unrecoverable storage exception.", e);
        new RuntimeHalterImpl().handleFatal(e);
      } catch (Exception e) {
        LOG.error("FATAL: Unhandled exception ", e);
        new RuntimeHalterImpl().handleFatal(e);
      }
    }
    kafkaConsumer.close();
  }

  /**
   * The only way we can ensure durability of data is when the data _and_ metadata are stored
   * reliably. So, on a indexer shutdown we need to ensure that as much of indexed data and metadata
   * is stored reliably. Otherwise, on an indexer shutdown we would end up re-indexing the data
   * which would result in wasted work.
   *
   * <p>The best way to close an indexer is the following steps: stop ingestion, index the ingested
   * messages, persist the indexed messages and metadata successfully and then close the
   * chunkManager and then the consumer.
   */
  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down Kaldb indexer.");

    // Shutdown kafka consumer
    try {
      kafkaConsumer.close();
    } catch (Exception e) {
      LOG.warn("Failed to close kafka consumer cleanly because of an exception.", e);
    }

    // We don't need to explicitly close the chunkManager, as it will attempt to close itself and
    // will persist the appropriate offset into ZK if it can complete an upload in time

    LOG.info("Kaldb indexer is closed.");
  }

  @Override
  protected String serviceName() {
    return "kaldbIndexerService";
  }
}
