package com.slack.kaldb.recovery;

import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.util.TimeUtils.nanosToMillis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.TextFormat;
import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.chunkManager.RecoveryChunkManager;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.core.KaldbMetadataStoreChangeListener;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.writer.LogMessageWriterImpl;
import com.slack.kaldb.writer.kafka.KaldbKafkaConsumer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The recovery service is intended to be executed on a recovery node, and is responsible for
 * fulfilling recovery assignments provided from the cluster manager.
 *
 * <p>When the recovery service starts it advertises its availability by creating a recovery node,
 * and then subscribing to any state changes. Upon receiving an assignment from the cluster manager
 * the recovery service will delegate the recovery task to an executor service. Once the recovery
 * task has been completed, the recovery node will make itself available again for assignment.
 *
 * <p>Look at handleRecoveryTaskAssignment method understand the implementation and limitations of
 * the current implementation.
 */
public class RecoveryService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(RecoveryService.class);

  private final SearchContext searchContext;
  private final AsyncCuratorFramework curatorFramework;
  private final MeterRegistry meterRegistry;
  private final BlobFs blobFs;
  private final KaldbConfigs.KaldbConfig kaldbConfig;
  private final AdminClient adminClient;

  private RecoveryNodeMetadataStore recoveryNodeMetadataStore;
  private RecoveryNodeMetadataStore recoveryNodeListenerMetadataStore;
  private RecoveryTaskMetadataStore recoveryTaskMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private final ExecutorService executorService;

  private Metadata.RecoveryNodeMetadata.RecoveryNodeState recoveryNodeLastKnownState;

  public static final String RECOVERY_NODE_ASSIGNMENT_RECEIVED =
      "recovery_node_assignment_received";
  public static final String RECOVERY_NODE_ASSIGNMENT_SUCCESS = "recovery_node_assignment_success";
  public static final String RECOVERY_NODE_ASSIGNMENT_FAILED = "recovery_node_assignment_failed";
  public static final String RECORDS_NO_LONGER_AVAILABLE = "records_no_longer_available";
  public static final String RECOVERY_TASK_TIMER = "recovery_task_timer";
  protected final Counter recoveryNodeAssignmentReceived;
  protected final Counter recoveryNodeAssignmentSuccess;
  protected final Counter recoveryNodeAssignmentFailed;
  protected final Counter recoveryRecordsNoLongerAvailable;
  private final Timer recoveryTaskTimerSuccess;
  private final Timer recoveryTaskTimerFailure;
  private SearchMetadataStore searchMetadataStore;

  private final KaldbMetadataStoreChangeListener<RecoveryNodeMetadata> recoveryNodeListener =
      this::recoveryNodeListener;

  public RecoveryService(
      KaldbConfigs.KaldbConfig kaldbConfig,
      AsyncCuratorFramework curatorFramework,
      MeterRegistry meterRegistry,
      BlobFs blobFs) {
    this.curatorFramework = curatorFramework;
    this.searchContext =
        SearchContext.fromConfig(kaldbConfig.getRecoveryConfig().getServerConfig());
    this.meterRegistry = meterRegistry;
    this.blobFs = blobFs;
    this.kaldbConfig = kaldbConfig;

    adminClient =
        AdminClient.create(
            Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                kaldbConfig.getRecoveryConfig().getKafkaConfig().getKafkaBootStrapServers(),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                "5000"));

    // we use a single thread executor to allow operations for this recovery node to queue,
    // guaranteeing that they are executed in the order they were received
    this.executorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setUncaughtExceptionHandler(
                    (t, e) -> LOG.error("Exception on thread {}: {}", t.getName(), e))
                .setNameFormat("recovery-service-%d")
                .build());

    Collection<Tag> meterTags = ImmutableList.of(Tag.of("nodeHostname", searchContext.hostname));
    recoveryNodeAssignmentReceived =
        meterRegistry.counter(RECOVERY_NODE_ASSIGNMENT_RECEIVED, meterTags);
    recoveryNodeAssignmentSuccess =
        meterRegistry.counter(RECOVERY_NODE_ASSIGNMENT_SUCCESS, meterTags);
    recoveryNodeAssignmentFailed =
        meterRegistry.counter(RECOVERY_NODE_ASSIGNMENT_FAILED, meterTags);
    recoveryRecordsNoLongerAvailable =
        meterRegistry.counter(RECORDS_NO_LONGER_AVAILABLE, meterTags);
    recoveryTaskTimerSuccess = meterRegistry.timer(RECOVERY_TASK_TIMER, "successful", "true");
    recoveryTaskTimerFailure = meterRegistry.timer(RECOVERY_TASK_TIMER, "successful", "false");
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting recovery service");

    recoveryNodeMetadataStore =
        new RecoveryNodeMetadataStore(curatorFramework, false, meterRegistry);
    recoveryTaskMetadataStore =
        new RecoveryTaskMetadataStore(curatorFramework, false, meterRegistry);
    snapshotMetadataStore = new SnapshotMetadataStore(curatorFramework, meterRegistry);
    searchMetadataStore = new SearchMetadataStore(curatorFramework, false, meterRegistry);

    recoveryNodeMetadataStore.createSync(
        new RecoveryNodeMetadata(
            searchContext.hostname,
            Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
            "",
            Instant.now().toEpochMilli()));
    recoveryNodeLastKnownState = Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE;

    recoveryNodeListenerMetadataStore =
        new RecoveryNodeMetadataStore(
            curatorFramework, searchContext.hostname, true, meterRegistry);
    recoveryNodeListenerMetadataStore.addListener(recoveryNodeListener);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing the recovery service");

    recoveryNodeListenerMetadataStore.addListener(recoveryNodeListener);

    recoveryNodeMetadataStore.close();
    recoveryTaskMetadataStore.close();
    snapshotMetadataStore.close();
    searchMetadataStore.close();

    // Immediately shutdown recovery tasks. Any incomplete recovery tasks will be picked up by
    // another recovery node so we don't need to wait for processing to complete.
    executorService.shutdownNow();

    LOG.info("Closed the recovery service");
  }

  private void recoveryNodeListener(RecoveryNodeMetadata recoveryNodeMetadata) {
    Metadata.RecoveryNodeMetadata.RecoveryNodeState newRecoveryNodeState =
        recoveryNodeMetadata.recoveryNodeState;
    if (recoveryNodeLastKnownState.equals(recoveryNodeMetadata.recoveryNodeState)) {
      // todo - consider moving this to a model where it deduplicates with a scheduled executor,
      // similar to the manager
      // This can fire duplicate events if the ephemeral node is re-created
      LOG.info("Recovery node - listener fired with no state change, skipping event");
      return;
    }

    if (newRecoveryNodeState.equals(Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED)) {
      LOG.info("Recovery node - ASSIGNED received");
      recoveryNodeAssignmentReceived.increment();
      if (!recoveryNodeLastKnownState.equals(
          Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE)) {
        LOG.warn(
            "Unexpected state transition from {} to {}",
            recoveryNodeLastKnownState,
            newRecoveryNodeState);
      }
      executorService.execute(() -> handleRecoveryTaskAssignment(recoveryNodeMetadata));
    }
    recoveryNodeLastKnownState = newRecoveryNodeState;
  }

  /**
   * This method is invoked after the cluster manager has assigned a recovery node a task. As part
   * of handling a task assignment we update the recovery node state to recovering once we start the
   * recovery process. If the recovery succeeds, we delete the recovery task and set node to free.
   * If the recovery task fails, we set the node to free so the recovery task can be assigned to
   * another node again.
   *
   * <p>Currently, we expect each recovery task to create one chunk. We don't support multiple
   * chunks per recovery task for a few reasons. It keeps the recovery protocol very simple since we
   * don't have to deal with partial chunk upload failures. By creating only one chunk per recovery
   * task, the runtime and resource utilization of an individual recovery task is bounded and
   * predictable, so it's easy to plan capacity for it. We get implicit parallelism in execution by
   * adding more recovery nodes and there is no need for additional mechanisms for parallelizing
   * execution.
   *
   * <p>TODO: Re-queuing failed re-assignment task will lead to wasted resources if recovery always
   * fails. To break this cycle add a enqueue_count value to recovery task so we can stop recovering
   * it if the task fails a certain number of times.
   */
  protected void handleRecoveryTaskAssignment(RecoveryNodeMetadata recoveryNodeMetadata) {
    try {
      setRecoveryNodeMetadataState(Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING);
      RecoveryTaskMetadata recoveryTaskMetadata =
          recoveryTaskMetadataStore.getSync(recoveryNodeMetadata.recoveryTaskName);

      if (!isValidRecoveryTask(recoveryTaskMetadata)) {
        LOG.error(
            "Invalid recovery task detected, skipping and deleting invalid task {}",
            recoveryTaskMetadata);
        recoveryTaskMetadataStore.deleteSync(recoveryTaskMetadata.name);
        setRecoveryNodeMetadataState(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);
        recoveryNodeAssignmentFailed.increment();
      } else {
        boolean success = handleRecoveryTask(recoveryTaskMetadata);
        if (success) {
          // delete the completed recovery task on success
          recoveryTaskMetadataStore.deleteSync(recoveryTaskMetadata.name);
          setRecoveryNodeMetadataState(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);
          recoveryNodeAssignmentSuccess.increment();
        } else {
          setRecoveryNodeMetadataState(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);
          recoveryNodeAssignmentFailed.increment();
        }
      }
    } catch (Exception e) {
      setRecoveryNodeMetadataState(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);
      LOG.error("Failed to complete recovery node task assignment", e);
      recoveryNodeAssignmentFailed.increment();
    }
  }

  /**
   * Attempts a final sanity-check on the recovery task to prevent a bad task from halting the
   * recovery pipeline. Bad state should be ideally prevented at the creation, as well as prior to
   * assignment, but this can be considered a final fail-safe if invalid recovery tasks somehow made
   * it this far.
   */
  private boolean isValidRecoveryTask(RecoveryTaskMetadata recoveryTaskMetadata) {
    // todo - consider adding further invalid recovery task detections
    if (recoveryTaskMetadata.endOffset <= recoveryTaskMetadata.startOffset) {
      return false;
    }
    return true;
  }

  /**
   * This method does the recovery work from a recovery task. A recovery task indicates the start
   * and end offset of a kafka partition to index. To do the recovery work, we create a recovery
   * chunk manager, create a kafka consumer for the recovery partition, indexes the data in
   * parallel, uploads the data to S3 and closes all the components correctly. We return true if the
   * operation succeeded.
   */
  @VisibleForTesting
  boolean handleRecoveryTask(RecoveryTaskMetadata recoveryTaskMetadata) {
    LOG.info("Started handling the recovery task: {}", recoveryTaskMetadata);
    long startTime = System.nanoTime();
    Timer.Sample taskTimer = Timer.start(meterRegistry);

    PartitionOffsets partitionOffsets =
        validateKafkaOffsets(
            adminClient,
            recoveryTaskMetadata,
            kaldbConfig.getRecoveryConfig().getKafkaConfig().getKafkaTopic());
    long offsetsValidatedTime = System.nanoTime();
    long consumerPreparedTime = 0, messagesConsumedTime = 0, rolloversCompletedTime = 0;

    if (partitionOffsets != null) {
      RecoveryTaskMetadata validatedRecoveryTask =
          new RecoveryTaskMetadata(
              recoveryTaskMetadata.name,
              recoveryTaskMetadata.partitionId,
              partitionOffsets.startOffset,
              partitionOffsets.endOffset,
              recoveryTaskMetadata.createdTimeEpochMs);

      if (partitionOffsets.startOffset != recoveryTaskMetadata.startOffset
          || recoveryTaskMetadata.endOffset != partitionOffsets.endOffset) {
        recoveryRecordsNoLongerAvailable.increment(
            (partitionOffsets.startOffset - recoveryTaskMetadata.startOffset)
                + (partitionOffsets.endOffset - recoveryTaskMetadata.endOffset));
      }

      try {
        RecoveryChunkManager<LogMessage> chunkManager =
            RecoveryChunkManager.fromConfig(
                meterRegistry,
                searchMetadataStore,
                snapshotMetadataStore,
                kaldbConfig.getIndexerConfig(),
                blobFs,
                kaldbConfig.getS3Config());

        // Ingest data in parallel
        LogMessageWriterImpl logMessageWriterImpl = new LogMessageWriterImpl(chunkManager);
        KaldbKafkaConsumer kafkaConsumer =
            new KaldbKafkaConsumer(
                makeKafkaConfig(
                    kaldbConfig.getRecoveryConfig().getKafkaConfig(),
                    validatedRecoveryTask.partitionId),
                logMessageWriterImpl,
                meterRegistry);

        kafkaConsumer.prepConsumerForConsumption(validatedRecoveryTask.startOffset);
        consumerPreparedTime = System.nanoTime();
        kafkaConsumer.consumeMessagesBetweenOffsetsInParallel(
            KaldbKafkaConsumer.KAFKA_POLL_TIMEOUT_MS,
            validatedRecoveryTask.startOffset,
            validatedRecoveryTask.endOffset);
        messagesConsumedTime = System.nanoTime();
        // Wait for chunks to upload.
        boolean success = chunkManager.waitForRollOvers();
        rolloversCompletedTime = System.nanoTime();
        // Close the recovery chunk manager and kafka consumer.
        kafkaConsumer.close();
        chunkManager.stopAsync();
        chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
        LOG.info("Finished handling the recovery task: {}", validatedRecoveryTask);
        taskTimer.stop(recoveryTaskTimerSuccess);
        return success;
      } catch (Exception ex) {
        LOG.error("Exception in recovery task [{}]: {}", validatedRecoveryTask, ex);
        taskTimer.stop(recoveryTaskTimerFailure);
        return false;
      } finally {
        long endTime = System.nanoTime();
        LOG.info(
            "Recovery task {} took {}ms, (subtask times offset validation {}, consumer prep {}, msg consumption {}, rollover {})",
            recoveryTaskMetadata,
            nanosToMillis(endTime - startTime),
            nanosToMillis(offsetsValidatedTime - startTime),
            nanosToMillis(consumerPreparedTime - offsetsValidatedTime),
            nanosToMillis(messagesConsumedTime - consumerPreparedTime),
            nanosToMillis(rolloversCompletedTime - messagesConsumedTime));
      }
    } else {
      LOG.info(
          "Recovery task {} data no longer available in Kafka (validation time {}ms)",
          recoveryTaskMetadata,
          nanosToMillis(offsetsValidatedTime - startTime));
      recoveryRecordsNoLongerAvailable.increment(
          recoveryTaskMetadata.endOffset - recoveryTaskMetadata.startOffset + 1);
      return true;
    }
  }

  // Replace the Kafka PartitionId from the kafkaConfig added.
  private KaldbConfigs.KafkaConfig makeKafkaConfig(
      KaldbConfigs.KafkaConfig kafkaConfig, String partitionId) throws TextFormat.ParseException {
    KaldbConfigs.KafkaConfig.Builder builder = KaldbConfigs.KafkaConfig.newBuilder();
    TextFormat.merge(kafkaConfig.toString(), builder);
    builder.setKafkaTopicPartition(partitionId);
    return builder.build();
  }

  private void setRecoveryNodeMetadataState(
      Metadata.RecoveryNodeMetadata.RecoveryNodeState newRecoveryNodeState) {
    RecoveryNodeMetadata recoveryNodeMetadata =
        recoveryNodeMetadataStore.getSync(searchContext.hostname);
    RecoveryNodeMetadata updatedRecoveryNodeMetadata =
        new RecoveryNodeMetadata(
            recoveryNodeMetadata.name,
            newRecoveryNodeState,
            newRecoveryNodeState.equals(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE)
                ? ""
                : recoveryNodeMetadata.recoveryTaskName,
            Instant.now().toEpochMilli());
    recoveryNodeMetadataStore.updateSync(updatedRecoveryNodeMetadata);
  }

  /**
   * Adjusts the offsets from the recovery task based on the availability of the offsets in Kafka.
   * Returns <code>null</code> if the offsets specified in the recovery task are completely
   * unavailable in Kafka.
   */
  @VisibleForTesting
  static PartitionOffsets validateKafkaOffsets(
      AdminClient adminClient, RecoveryTaskMetadata recoveryTask, String kafkaTopic) {
    TopicPartition topicPartition =
        KaldbKafkaConsumer.getTopicPartition(kafkaTopic, recoveryTask.partitionId);
    long earliestKafkaOffset =
        getPartitionOffset(adminClient, topicPartition, OffsetSpec.earliest());
    long newStartOffset = recoveryTask.startOffset;
    long newEndOffset = recoveryTask.endOffset;

    if (earliestKafkaOffset > recoveryTask.endOffset) {
      LOG.warn(
          "Entire task range ({}-{}) on topic {} is unavailable in Kafka (earliest offset: {})",
          recoveryTask.startOffset,
          recoveryTask.endOffset,
          topicPartition,
          earliestKafkaOffset);
      return null;
    }

    long latestKafkaOffset = getPartitionOffset(adminClient, topicPartition, OffsetSpec.latest());
    if (latestKafkaOffset < recoveryTask.startOffset) {
      // this should never happen, but if it somehow did, it would result in an infinite
      // loop in the consumeMessagesBetweenOffsetsInParallel method
      LOG.warn(
          "Entire task range ({}-{}) on topic {} is unavailable in Kafka (latest offset: {})",
          recoveryTask.startOffset,
          recoveryTask.endOffset,
          topicPartition,
          latestKafkaOffset);
      return null;
    }

    if (recoveryTask.startOffset < earliestKafkaOffset) {
      LOG.warn(
          "Partial loss of messages in recovery task. Start offset {}, earliest available offset {}",
          recoveryTask.startOffset,
          earliestKafkaOffset);
      newStartOffset = earliestKafkaOffset;
    }
    if (recoveryTask.endOffset > latestKafkaOffset) {
      // this should never happen, but if it somehow did, the requested recovery range should
      // be adjusted down to the latest available offset in Kafka
      LOG.warn(
          "Partial loss of messages in recovery task. End offset {}, latest available offset {}",
          recoveryTask.endOffset,
          latestKafkaOffset);
      newEndOffset = latestKafkaOffset;
    }

    return new PartitionOffsets(newStartOffset, newEndOffset);
  }

  /**
   * Returns the specified offset (earliest, latest, timestamp) of the specified Kafka topic and
   * partition
   *
   * @return current offset or -1 if an error was encountered
   */
  @VisibleForTesting
  static long getPartitionOffset(
      AdminClient adminClient, TopicPartition topicPartition, OffsetSpec offsetSpec) {
    ListOffsetsResult offsetResults = adminClient.listOffsets(Map.of(topicPartition, offsetSpec));
    long offset = -1;
    try {
      offset = offsetResults.partitionResult(topicPartition).get().offset();
      return offset;
    } catch (Exception e) {
      LOG.error("Interrupted getting partition offset", e);
      return -1;
    }
  }

  static class PartitionOffsets {
    long startOffset;
    long endOffset;

    public PartitionOffsets(long startOffset, long endOffset) {
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }
  }
}
