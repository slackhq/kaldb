package com.slack.kaldb.server;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.blobfs.s3.S3BlobFsConfig;
import com.slack.kaldb.chunk.ChunkManager;
import com.slack.kaldb.chunk.ChunkRollOverStrategy;
import com.slack.kaldb.chunk.ChunkRollOverStrategyImpl;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.writer.LogMessageTransformer;
import com.slack.kaldb.writer.LogMessageWriterImpl;
import com.slack.kaldb.writer.kafka.KaldbKafkaWriter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KaldbIndexer sets up an indexer that indexes the log messages.
 *
 * <p>This indexer should be testable via junit tests. So, it should have the least number of deps
 * in it's constructor.
 *
 * <p>Single Binary for all Kaldb configured via command line flags.
 *
 * <p>Design should be extensible so we can run as separate components or all components in a single
 * binary.
 */
public class KaldbIndexer {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbIndexer.class);

  @VisibleForTesting
  static final Map<String, LogMessageTransformer> dataTransformerMap =
      ImmutableMap.of(
          "api_log",
          LogMessageWriterImpl.apiLogTransformer,
          "spans",
          LogMessageWriterImpl.spanTransformer,
          "json",
          LogMessageWriterImpl.jsonLogMessageTransformer);

  // TODO: Pass this in via config file.
  private static final String CHUNK_DATA_PREFIX = "log_data_";

  private final KaldbKafkaWriter kafkaWriter;

  public ChunkManager<LogMessage> getChunkManager() {
    return chunkManager;
  }

  private final ChunkManager<LogMessage> chunkManager;

  static KaldbIndexer fromConfig(MeterRegistry meterRegistry) {
    ChunkRollOverStrategy chunkRollOverStrategy = ChunkRollOverStrategyImpl.fromConfig();

    // TODO: Read the config values for chunk manager from config file.
    ChunkManager<LogMessage> chunkManager =
        new ChunkManager<>(
            CHUNK_DATA_PREFIX,
            KaldbConfig.get().getIndexerConfig().getDataDirectory(),
            chunkRollOverStrategy,
            meterRegistry,
            getS3BlobFsClient(KaldbConfig.get()),
            KaldbConfig.get().getS3Config().getS3Bucket(),
            ChunkManager.makeDefaultRollOverExecutor(),
            ChunkManager.DEFAULT_ROLLOVER_FUTURE_TIMEOUT_MS);

    String dataTransformerConfig = KaldbConfig.get().getIndexerConfig().getDataTransformer();
    if (dataTransformerConfig.isEmpty()) {
      throw new RuntimeException("IndexerConfig can't have an empty dataTransformer config.");
    }
    LogMessageTransformer dataTransformer = dataTransformerMap.get(dataTransformerConfig);
    if (dataTransformer == null) {
      throw new RuntimeException("Invalid data transformer config: " + dataTransformerConfig);
    }
    return new KaldbIndexer(chunkManager, dataTransformer, meterRegistry);
  }

  private static S3BlobFs getS3BlobFsClient(KaldbConfigs.KaldbConfig kaldbCfg) {
    KaldbConfigs.S3Config s3Config = kaldbCfg.getS3Config();
    S3BlobFsConfig s3BlobFsConfig =
        new S3BlobFsConfig(
            s3Config.getS3AccessKey(),
            s3Config.getS3SecretKey(),
            s3Config.getS3Region(),
            s3Config.getS3EndPoint());
    S3BlobFs s3BlobFs = new S3BlobFs();
    s3BlobFs.init(s3BlobFsConfig);
    return s3BlobFs;
  }

  /**
   * This class contains the code to needed to run a single instance of an Kaldb indexer. A single
   * instance of Kaldb indexer, indexes data from kafka into the chunk manager and provides an API
   * to search that data.
   *
   * <p>In addition, this class also contains the code to gracefully start and shutdown the server.
   *
   * <p>The only way we can ensure durability of data is when the data _and_ metadata are stored
   * reliably. So, on a clean indexer shutdown we need to ensure that as much of indexed data and
   * metadata is stored reliably. Otherwise, on an indexer shutdown we would end up re-indexing the
   * data which would result in a lot of wasted work. *
   *
   * <p>On an indexer restart, we should start indexing at a last known good offset for that
   * partition. If a last known good offset doesn't exist since we are consuming for the first time
   * then we start with head. If the offset exists but the offset expired, we are in a whole world
   * of pain. The best option may to start indexing at oldest. Or we can also start indexing at
   * head.
   *
   * <p>Currently, we don't have a durable metadata store and the kafka consumer offset acts as a
   * weak place holder. On an indexer shutdown it is very important that we ensure that we persisted
   * * the offset of the data correctly. So we can pick up from the same location and start from
   * that place.
   *
   * <p>The best way to close an indexer is the following steps: stop ingestion, index the ingested
   * messages, persist the indexed messages and metadata successfully and then close the
   * chunkManager and then the consumer,
   */
  public KaldbIndexer(
      ChunkManager<LogMessage> chunkManager,
      LogMessageTransformer messageTransformer,
      MeterRegistry meterRegistry) {
    checkNotNull(chunkManager, "Chunk manager can't be null");
    this.chunkManager = chunkManager;

    LogMessageWriterImpl logMessageWriterImpl =
        new LogMessageWriterImpl(chunkManager, messageTransformer);
    kafkaWriter = KaldbKafkaWriter.fromConfig(logMessageWriterImpl, meterRegistry);
  }

  public void start() {
    LOG.info("Starting indexing into Kaldb.");
    kafkaWriter.start();
  }

  /**
   * TODO: Currently, we close the consumer at the same time as stopping indexing. It may be better
   * to separate those steps where we stop ingestion and then close the consumer separately. This
   * will help with cleaner indexing.
   */
  public void close() {
    LOG.info("Shutting down Kaldb indexer.");

    // Shutdown kafka consumer cleanly and then the chunkmanager so we can be sure, we have indexed
    // the data we ingested.
    ListenableFuture<?> kafkaFuture = kafkaWriter.triggerShutdown();
    try {
      LOG.info("Waiting for Kafka consumer to close.");
      // Use a more configurable timeout value.
      kafkaFuture.get(2, TimeUnit.SECONDS);
      if (kafkaFuture.isDone()) {
        LOG.info("Closed Kafka consumer cleanly");
      } else {
        LOG.warn("Kafka consumer was not closed cleanly");
      }
    } catch (TimeoutException e) {
      LOG.warn("Failed to close kafka consumer cleanly because of a timeout.", e);
    } catch (Exception e) {
      LOG.warn("Failed to close kafka consumer cleanly because of an exception.", e);
    }

    chunkManager.close();
    LOG.info("Kaldb indexer is closed.");
  }
}
