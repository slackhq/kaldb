package com.slack.kaldb.recovery;

import static com.slack.kaldb.chunkManager.RollOverChunkTask.ROLLOVERS_COMPLETED;
import static com.slack.kaldb.chunkManager.RollOverChunkTask.ROLLOVERS_FAILED;
import static com.slack.kaldb.chunkManager.RollOverChunkTask.ROLLOVERS_INITIATED;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.recovery.RecoveryService.RECORDS_NO_LONGER_AVAILABLE;
import static com.slack.kaldb.recovery.RecoveryService.RECOVERY_NODE_ASSIGNMENT_FAILED;
import static com.slack.kaldb.recovery.RecoveryService.RECOVERY_NODE_ASSIGNMENT_RECEIVED;
import static com.slack.kaldb.recovery.RecoveryService.RECOVERY_NODE_ASSIGNMENT_SUCCESS;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TestKafkaServer.produceMessagesToKafka;
import static com.slack.kaldb.writer.kafka.KaldbKafkaConsumerTest.TEST_KAFKA_CLIENT_GROUP;
import static com.slack.kaldb.writer.kafka.KaldbKafkaConsumerTest.getKafkaTestServer;
import static com.slack.kaldb.writer.kafka.KaldbKafkaConsumerTest.getStartOffset;
import static com.slack.kaldb.writer.kafka.KaldbKafkaConsumerTest.setRetentionTime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.google.common.collect.Maps;
import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.blobfs.s3.S3CrtBlobFs;
import com.slack.kaldb.blobfs.s3.S3TestUtils;
import com.slack.kaldb.logstore.BlobFsUtils;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.core.KaldbMetadataTestUtils;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import com.slack.kaldb.writer.kafka.KaldbKafkaConsumer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class RecoveryServiceTest {

  private static final String TEST_S3_BUCKET = "test-s3-bucket";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(TEST_S3_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  private static final String TEST_KAFKA_TOPIC_1 = "test-topic-1";
  private static final String KALDB_TEST_CLIENT_1 = "kaldb-test-client1";

  private TestingServer zkServer;
  private MeterRegistry meterRegistry;
  private BlobFs blobFs;
  private TestKafkaServer kafkaServer;
  private S3AsyncClient s3AsyncClient;
  private RecoveryService recoveryService;
  private AsyncCuratorFramework curatorFramework;

  @BeforeEach
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    kafkaServer = new TestKafkaServer();
    meterRegistry = new SimpleMeterRegistry();
    zkServer = new TestingServer();
    s3AsyncClient = S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
    blobFs = new S3CrtBlobFs(s3AsyncClient);
  }

  @AfterEach
  public void shutdown() throws Exception {
    if (recoveryService != null) {
      recoveryService.stopAsync();
      recoveryService.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    if (curatorFramework != null) {
      curatorFramework.unwrap().close();
    }
    if (blobFs != null) {
      blobFs.close();
    }
    if (kafkaServer != null) {
      kafkaServer.close();
    }
    if (zkServer != null) {
      zkServer.close();
    }
    if (meterRegistry != null) {
      meterRegistry.close();
    }
    if (s3AsyncClient != null) {
      s3AsyncClient.close();
    }
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  private KaldbConfigs.KaldbConfig makeKaldbConfig(String testS3Bucket) {
    return makeKaldbConfig(kafkaServer, testS3Bucket, RecoveryServiceTest.TEST_KAFKA_TOPIC_1);
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  private KaldbConfigs.KaldbConfig makeKaldbConfig(
      TestKafkaServer testKafkaServer, String testS3Bucket, String topic) {
    return KaldbConfigUtil.makeKaldbConfig(
        "localhost:" + testKafkaServer.getBroker().getKafkaPort().get(),
        9000,
        topic,
        0,
        RecoveryServiceTest.KALDB_TEST_CLIENT_1,
        testS3Bucket,
        9000 + 1,
        zkServer.getConnectString(),
        "recoveryZK_",
        KaldbConfigs.NodeRole.RECOVERY,
        10000,
        9003,
        100);
  }

  @Test
  public void testShouldHandleRecoveryTask() throws Exception {
    KaldbConfigs.KaldbConfig kaldbCfg = makeKaldbConfig(TEST_S3_BUCKET);
    curatorFramework =
        CuratorBuilder.build(meterRegistry, kaldbCfg.getMetadataStoreConfig().getZookeeperConfig());

    // Start recovery service
    recoveryService = new RecoveryService(kaldbCfg, curatorFramework, meterRegistry, blobFs);
    recoveryService.startAsync();
    recoveryService.awaitRunning(DEFAULT_START_STOP_DURATION);

    // Populate data in  Kafka so we can recover from it.
    final Instant startTime = Instant.now();
    produceMessagesToKafka(kafkaServer.getBroker(), startTime, TEST_KAFKA_TOPIC_1, 0);

    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, meterRegistry);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isZero();
    // Start recovery
    RecoveryTaskMetadata recoveryTask =
        new RecoveryTaskMetadata("testRecoveryTask", "0", 30, 60, Instant.now().toEpochMilli());
    assertThat(recoveryService.handleRecoveryTask(recoveryTask)).isTrue();
    List<SnapshotMetadata> snapshots =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshots.size()).isEqualTo(1);
    assertThat(blobFs.listFiles(BlobFsUtils.createURI(TEST_S3_BUCKET, "/", ""), true)).isNotEmpty();
    assertThat(blobFs.exists(URI.create(snapshots.get(0).snapshotPath))).isTrue();
    assertThat(blobFs.listFiles(URI.create(snapshots.get(0).snapshotPath), false).length)
        .isGreaterThan(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, meterRegistry)).isEqualTo(31);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, meterRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, meterRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, meterRegistry)).isEqualTo(0);

    snapshotMetadataStore.close();
  }

  @Test
  public void testShouldHandleRecoveryTaskWithCompletelyUnavailableOffsets() throws Exception {
    final TopicPartition topicPartition = new TopicPartition(TestKafkaServer.TEST_KAFKA_TOPIC, 0);
    TestKafkaServer.KafkaComponents components = getKafkaTestServer(S3_MOCK_EXTENSION);
    KaldbConfigs.KaldbConfig kaldbCfg =
        makeKaldbConfig(components.testKafkaServer, TEST_S3_BUCKET, topicPartition.topic());
    curatorFramework =
        CuratorBuilder.build(meterRegistry, kaldbCfg.getMetadataStoreConfig().getZookeeperConfig());

    KaldbConfigs.KafkaConfig kafkaConfig =
        KaldbConfigs.KafkaConfig.newBuilder()
            .setKafkaTopic(topicPartition.topic())
            .setKafkaTopicPartition(Integer.toString(topicPartition.partition()))
            .setKafkaBootStrapServers(components.testKafkaServer.getBroker().getBrokerList().get())
            .setKafkaClientGroup(TEST_KAFKA_CLIENT_GROUP)
            .setEnableKafkaAutoCommit("true")
            .setKafkaAutoCommitInterval("500")
            .setKafkaSessionTimeout("500")
            .putAllAdditionalProps(Maps.fromProperties(components.consumerOverrideProps))
            .build();

    final KaldbKafkaConsumer localTestConsumer =
        new KaldbKafkaConsumer(kafkaConfig, components.logMessageWriter, components.meterRegistry);
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final long msgsToProduce = 100;
    TestKafkaServer.produceMessagesToKafka(
        components.testKafkaServer.getBroker(),
        startTime,
        topicPartition.topic(),
        topicPartition.partition(),
        (int) msgsToProduce);
    await().until(() -> localTestConsumer.getEndOffSetForPartition() == msgsToProduce);
    // we immediately force delete the messages, as this is faster than changing the retention and
    // waiting for the cleaner to run
    components
        .adminClient
        .deleteRecords(Map.of(topicPartition, RecordsToDelete.beforeOffset(100)))
        .all()
        .get();
    assertThat(getStartOffset(components.adminClient, topicPartition)).isGreaterThan(0);

    // produce some more messages that won't be expired
    setRetentionTime(components.adminClient, topicPartition.topic(), 25000);
    TestKafkaServer.produceMessagesToKafka(
        components.testKafkaServer.getBroker(),
        startTime,
        topicPartition.topic(),
        topicPartition.partition(),
        (int) msgsToProduce);
    await()
        .until(() -> localTestConsumer.getEndOffSetForPartition() == msgsToProduce + msgsToProduce);

    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, meterRegistry);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isZero();

    // Start recovery service
    recoveryService =
        new RecoveryService(kaldbCfg, curatorFramework, components.meterRegistry, blobFs);
    recoveryService.startAsync();
    recoveryService.awaitRunning(DEFAULT_START_STOP_DURATION);
    long startOffset = 1;
    long endOffset = msgsToProduce - 1;
    RecoveryTaskMetadata recoveryTask =
        new RecoveryTaskMetadata(
            topicPartition.topic(),
            Integer.toString(topicPartition.partition()),
            startOffset,
            endOffset,
            Instant.now().toEpochMilli());
    assertThat(recoveryService.handleRecoveryTask(recoveryTask)).isTrue();
    assertThat(getCount(RECORDS_NO_LONGER_AVAILABLE, components.meterRegistry))
        .isEqualTo(endOffset - startOffset + 1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, components.meterRegistry)).isEqualTo(0);
    List<SnapshotMetadata> snapshots =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshots.size()).isEqualTo(0);
    assertThat(blobFs.listFiles(BlobFsUtils.createURI(TEST_S3_BUCKET, "/", ""), true)).isEmpty();
    assertThat(getCount(MESSAGES_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_FAILED, meterRegistry)).isEqualTo(0);

    snapshotMetadataStore.close();
  }

  @Test
  public void testShouldHandleRecoveryTaskWithPartiallyUnavailableOffsets() throws Exception {
    final TopicPartition topicPartition = new TopicPartition(TestKafkaServer.TEST_KAFKA_TOPIC, 0);
    TestKafkaServer.KafkaComponents components = getKafkaTestServer(S3_MOCK_EXTENSION);
    KaldbConfigs.KaldbConfig kaldbCfg =
        makeKaldbConfig(components.testKafkaServer, TEST_S3_BUCKET, topicPartition.topic());
    curatorFramework =
        CuratorBuilder.build(meterRegistry, kaldbCfg.getMetadataStoreConfig().getZookeeperConfig());

    KaldbConfigs.KafkaConfig kafkaConfig =
        KaldbConfigs.KafkaConfig.newBuilder()
            .setKafkaTopic(topicPartition.topic())
            .setKafkaTopicPartition(Integer.toString(topicPartition.partition()))
            .setKafkaBootStrapServers(components.testKafkaServer.getBroker().getBrokerList().get())
            .setKafkaClientGroup(TEST_KAFKA_CLIENT_GROUP)
            .setEnableKafkaAutoCommit("true")
            .setKafkaAutoCommitInterval("500")
            .setKafkaSessionTimeout("500")
            .putAllAdditionalProps(Maps.fromProperties(components.consumerOverrideProps))
            .build();

    final KaldbKafkaConsumer localTestConsumer =
        new KaldbKafkaConsumer(kafkaConfig, components.logMessageWriter, components.meterRegistry);
    final Instant startTime = Instant.now();
    final long msgsToProduce = 100;
    TestKafkaServer.produceMessagesToKafka(
        components.testKafkaServer.getBroker(),
        startTime,
        topicPartition.topic(),
        topicPartition.partition(),
        (int) msgsToProduce);
    await().until(() -> localTestConsumer.getEndOffSetForPartition() == msgsToProduce);
    // we immediately force delete the messages, as this is faster than changing the retention and
    // waiting for the cleaner to run
    components
        .adminClient
        .deleteRecords(Map.of(topicPartition, RecordsToDelete.beforeOffset(100)))
        .all()
        .get();
    assertThat(getStartOffset(components.adminClient, topicPartition)).isGreaterThan(0);

    // produce some more messages that won't be expired
    setRetentionTime(components.adminClient, topicPartition.topic(), 25000);
    TestKafkaServer.produceMessagesToKafka(
        components.testKafkaServer.getBroker(),
        startTime,
        topicPartition.topic(),
        topicPartition.partition(),
        (int) msgsToProduce);
    await()
        .until(() -> localTestConsumer.getEndOffSetForPartition() == msgsToProduce + msgsToProduce);

    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, meterRegistry);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isZero();

    // Start recovery service
    recoveryService =
        new RecoveryService(kaldbCfg, curatorFramework, components.meterRegistry, blobFs);
    recoveryService.startAsync();
    recoveryService.awaitRunning(DEFAULT_START_STOP_DURATION);

    // Start recovery with an offset range that is partially unavailable
    long startOffset = 50;
    long endOffset = 150;
    RecoveryTaskMetadata recoveryTask =
        new RecoveryTaskMetadata(
            topicPartition.topic(),
            Integer.toString(topicPartition.partition()),
            startOffset,
            endOffset,
            Instant.now().toEpochMilli());
    assertThat(recoveryService.handleRecoveryTask(recoveryTask)).isTrue();
    assertThat(getCount(RECORDS_NO_LONGER_AVAILABLE, components.meterRegistry)).isEqualTo(50);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, components.meterRegistry)).isEqualTo(51);
    List<SnapshotMetadata> snapshots =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshots.size()).isEqualTo(1);
    assertThat(blobFs.listFiles(BlobFsUtils.createURI(TEST_S3_BUCKET, "/", ""), true)).isNotEmpty();
    assertThat(blobFs.exists(URI.create(snapshots.get(0).snapshotPath))).isTrue();
    assertThat(blobFs.listFiles(URI.create(snapshots.get(0).snapshotPath), false).length)
        .isGreaterThan(1);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_FAILED, meterRegistry)).isEqualTo(0);

    snapshotMetadataStore.close();
  }

  @Test
  public void testShouldHandleRecoveryTaskFailure() throws Exception {
    String fakeS3Bucket = "fakeBucket";
    KaldbConfigs.KaldbConfig kaldbCfg = makeKaldbConfig(fakeS3Bucket);
    curatorFramework =
        CuratorBuilder.build(meterRegistry, kaldbCfg.getMetadataStoreConfig().getZookeeperConfig());

    // Start recovery service
    recoveryService = new RecoveryService(kaldbCfg, curatorFramework, meterRegistry, blobFs);
    recoveryService.startAsync();
    recoveryService.awaitRunning(DEFAULT_START_STOP_DURATION);

    // Populate data in  Kafka so we can recover from it.
    final Instant startTime = Instant.now();
    produceMessagesToKafka(kafkaServer.getBroker(), startTime, TEST_KAFKA_TOPIC_1, 0);

    assertThat(s3AsyncClient.listBuckets().get().buckets().size()).isEqualTo(1);
    assertThat(s3AsyncClient.listBuckets().get().buckets().get(0).name()).isEqualTo(TEST_S3_BUCKET);
    assertThat(s3AsyncClient.listBuckets().get().buckets().get(0).name())
        .isNotEqualTo(fakeS3Bucket);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, meterRegistry);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isZero();

    // Start recovery
    RecoveryTaskMetadata recoveryTask =
        new RecoveryTaskMetadata("testRecoveryTask", "0", 30, 60, Instant.now().toEpochMilli());
    assertThat(recoveryService.handleRecoveryTask(recoveryTask)).isFalse();

    assertThat(s3AsyncClient.listBuckets().get().buckets().size()).isEqualTo(1);
    assertThat(s3AsyncClient.listBuckets().get().buckets().get(0).name()).isEqualTo(TEST_S3_BUCKET);
    assertThat(s3AsyncClient.listBuckets().get().buckets().get(0).name())
        .isNotEqualTo(fakeS3Bucket);

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, meterRegistry)).isEqualTo(31);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, meterRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_FAILED, meterRegistry)).isEqualTo(1);

    snapshotMetadataStore.close();
  }

  @Test
  public void testShouldHandleRecoveryTaskAssignmentSuccess() throws Exception {
    KaldbConfigs.KaldbConfig kaldbCfg = makeKaldbConfig(TEST_S3_BUCKET);
    curatorFramework =
        CuratorBuilder.build(meterRegistry, kaldbCfg.getMetadataStoreConfig().getZookeeperConfig());

    // Start recovery service
    recoveryService = new RecoveryService(kaldbCfg, curatorFramework, meterRegistry, blobFs);
    recoveryService.startAsync();
    recoveryService.awaitRunning(DEFAULT_START_STOP_DURATION);

    // Populate data in  Kafka so we can recover data from Kafka.
    final Instant startTime = Instant.now();
    produceMessagesToKafka(kafkaServer.getBroker(), startTime, TEST_KAFKA_TOPIC_1, 0);

    assertThat(s3AsyncClient.listBuckets().get().buckets().size()).isEqualTo(1);
    assertThat(s3AsyncClient.listBuckets().get().buckets().get(0).name()).isEqualTo(TEST_S3_BUCKET);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, meterRegistry);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isZero();

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isZero();
    // Create a recovery task
    RecoveryTaskMetadataStore recoveryTaskMetadataStore =
        new RecoveryTaskMetadataStore(curatorFramework, false, meterRegistry);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).size()).isZero();
    RecoveryTaskMetadata recoveryTask =
        new RecoveryTaskMetadata("testRecoveryTask", "0", 30, 60, Instant.now().toEpochMilli());
    recoveryTaskMetadataStore.createSync(recoveryTask);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).size())
        .isEqualTo(1);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).get(0))
        .isEqualTo(recoveryTask);

    // Assign the recovery task to node.
    RecoveryNodeMetadataStore recoveryNodeMetadataStore =
        new RecoveryNodeMetadataStore(curatorFramework, false, meterRegistry);
    List<RecoveryNodeMetadata> recoveryNodes =
        KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore);
    assertThat(recoveryNodes.size()).isEqualTo(1);
    RecoveryNodeMetadata recoveryNodeMetadata = recoveryNodes.get(0);
    assertThat(recoveryNodeMetadata.recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);
    recoveryNodeMetadataStore.updateSync(
        new RecoveryNodeMetadata(
            recoveryNodeMetadata.getName(),
            Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED,
            recoveryTask.getName(),
            Instant.now().toEpochMilli()));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).size())
        .isEqualTo(1);

    await().until(() -> getCount(RECOVERY_NODE_ASSIGNMENT_SUCCESS, meterRegistry) == 1);
    assertThat(getCount(RECOVERY_NODE_ASSIGNMENT_RECEIVED, meterRegistry)).isEqualTo(1);
    assertThat(getCount(RECOVERY_NODE_ASSIGNMENT_FAILED, meterRegistry)).isZero();

    // Check metadata
    assertThat(s3AsyncClient.listBuckets().get().buckets().size()).isEqualTo(1);
    assertThat(s3AsyncClient.listBuckets().get().buckets().get(0).name()).isEqualTo(TEST_S3_BUCKET);

    // Post recovery checks
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore).size())
        .isEqualTo(1);
    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore)
                .get(0)
                .recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);

    // 1 snapshot is published
    List<SnapshotMetadata> snapshots =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isEqualTo(1);
    assertThat(blobFs.exists(URI.create(snapshots.get(0).snapshotPath))).isTrue();
    assertThat(blobFs.listFiles(URI.create(snapshots.get(0).snapshotPath), false).length)
        .isGreaterThan(1);

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, meterRegistry)).isEqualTo(31);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, meterRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, meterRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, meterRegistry)).isEqualTo(0);

    snapshotMetadataStore.close();
    recoveryTaskMetadataStore.close();
    recoveryNodeMetadataStore.close();
  }

  @Test
  public void testShouldHandleRecoveryTaskAssignmentFailure() throws Exception {
    String fakeS3Bucket = "fakeS3Bucket";
    KaldbConfigs.KaldbConfig kaldbCfg = makeKaldbConfig(fakeS3Bucket);
    curatorFramework =
        CuratorBuilder.build(meterRegistry, kaldbCfg.getMetadataStoreConfig().getZookeeperConfig());

    // Start recovery service
    recoveryService = new RecoveryService(kaldbCfg, curatorFramework, meterRegistry, blobFs);
    recoveryService.startAsync();
    recoveryService.awaitRunning(DEFAULT_START_STOP_DURATION);

    // Populate data in  Kafka so we can recover data from Kafka.
    final Instant startTime = Instant.now();
    produceMessagesToKafka(kafkaServer.getBroker(), startTime, TEST_KAFKA_TOPIC_1, 0);

    // fakeS3Bucket is not present.
    assertThat(s3AsyncClient.listBuckets().get().buckets().size()).isEqualTo(1);
    assertThat(s3AsyncClient.listBuckets().get().buckets().get(0).name()).isEqualTo(TEST_S3_BUCKET);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, meterRegistry);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isZero();

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isZero();
    // Create a recovery task
    RecoveryTaskMetadataStore recoveryTaskMetadataStore =
        new RecoveryTaskMetadataStore(curatorFramework, false, meterRegistry);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).size()).isZero();
    RecoveryTaskMetadata recoveryTask =
        new RecoveryTaskMetadata("testRecoveryTask", "0", 30, 60, Instant.now().toEpochMilli());
    recoveryTaskMetadataStore.createSync(recoveryTask);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).size())
        .isEqualTo(1);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).get(0))
        .isEqualTo(recoveryTask);

    // Assign the recovery task to node.
    RecoveryNodeMetadataStore recoveryNodeMetadataStore =
        new RecoveryNodeMetadataStore(curatorFramework, false, meterRegistry);
    List<RecoveryNodeMetadata> recoveryNodes =
        KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore);
    assertThat(recoveryNodes.size()).isEqualTo(1);
    RecoveryNodeMetadata recoveryNodeMetadata = recoveryNodes.get(0);
    assertThat(recoveryNodeMetadata.recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);
    recoveryNodeMetadataStore.updateSync(
        new RecoveryNodeMetadata(
            recoveryNodeMetadata.getName(),
            Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED,
            recoveryTask.getName(),
            Instant.now().toEpochMilli()));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).size())
        .isEqualTo(1);

    await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> getCount(RECOVERY_NODE_ASSIGNMENT_FAILED, meterRegistry) == 1);
    assertThat(getCount(RECOVERY_NODE_ASSIGNMENT_RECEIVED, meterRegistry)).isEqualTo(1);
    assertThat(getCount(RECOVERY_NODE_ASSIGNMENT_SUCCESS, meterRegistry)).isZero();

    // Check metadata
    assertThat(s3AsyncClient.listBuckets().get().buckets().size()).isEqualTo(1);
    assertThat(s3AsyncClient.listBuckets().get().buckets().get(0).name()).isEqualTo(TEST_S3_BUCKET);

    // Post recovery checks
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore).size())
        .isEqualTo(1);
    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore)
                .get(0)
                .recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);

    // Recovery task still exists for re-assignment.
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).size())
        .isEqualTo(1);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).get(0))
        .isEqualTo(recoveryTask);

    // No snapshots are published on failure.
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isZero();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, meterRegistry)).isEqualTo(31);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, meterRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, meterRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_FAILED, meterRegistry)).isEqualTo(1);

    snapshotMetadataStore.close();
    recoveryTaskMetadataStore.close();
    recoveryNodeMetadataStore.close();
  }

  @Test
  public void testValidateOffsetsWhenRecoveryTaskEntirelyAvailableInKafka() {
    long kafkaStartOffset = 100;
    long kafkaEndOffset = 900;
    long recoveryTaskStartOffset = 200;
    long recoveryTaskEndOffset = 300;
    String topic = "foo";

    RecoveryService.PartitionOffsets offsets =
        RecoveryService.validateKafkaOffsets(
            getAdminClient(kafkaStartOffset, kafkaEndOffset),
            new RecoveryTaskMetadata("foo", "1", recoveryTaskStartOffset, recoveryTaskEndOffset, 1),
            topic);

    assertThat(offsets.startOffset).isEqualTo(recoveryTaskStartOffset);
    assertThat(offsets.endOffset).isEqualTo(recoveryTaskEndOffset);
  }

  @Test
  public void testValidateOffsetsWhenRecoveryTaskOverlapsWithBeginningOfKafkaRange() {
    long kafkaStartOffset = 100;
    long kafkaEndOffset = 900;
    long recoveryTaskStartOffset = 50;
    long recoveryTaskEndOffset = 300;
    String topic = "foo";

    RecoveryService.PartitionOffsets offsets =
        RecoveryService.validateKafkaOffsets(
            getAdminClient(kafkaStartOffset, kafkaEndOffset),
            new RecoveryTaskMetadata("foo", "1", recoveryTaskStartOffset, recoveryTaskEndOffset, 1),
            topic);

    assertThat(offsets.startOffset).isEqualTo(kafkaStartOffset);
    assertThat(offsets.endOffset).isEqualTo(recoveryTaskEndOffset);
  }

  @Test
  public void testValidateOffsetsWhenRecoveryTaskBeforeKafkaRange() {
    long kafkaStartOffset = 100;
    long kafkaEndOffset = 900;
    long recoveryTaskStartOffset = 1;
    long recoveryTaskEndOffset = 50;
    String topic = "foo";

    RecoveryService.PartitionOffsets offsets =
        RecoveryService.validateKafkaOffsets(
            getAdminClient(kafkaStartOffset, kafkaEndOffset),
            new RecoveryTaskMetadata("foo", "1", recoveryTaskStartOffset, recoveryTaskEndOffset, 1),
            topic);

    assertThat(offsets).isNull();
  }

  @Test
  public void testValidateOffsetsWhenRecoveryTaskAfterKafkaRange() {
    long kafkaStartOffset = 100;
    long kafkaEndOffset = 900;
    long recoveryTaskStartOffset = 1000;
    long recoveryTaskEndOffset = 5000;
    String topic = "foo";

    RecoveryService.PartitionOffsets offsets =
        RecoveryService.validateKafkaOffsets(
            getAdminClient(kafkaStartOffset, kafkaEndOffset),
            new RecoveryTaskMetadata("foo", "1", recoveryTaskStartOffset, recoveryTaskEndOffset, 1),
            topic);

    assertThat(offsets).isNull();
  }

  @Test
  public void testValidateOffsetsWhenRecoveryTaskOverlapsWithEndOfKafkaRange() {
    long kafkaStartOffset = 100;
    long kafkaEndOffset = 900;
    long recoveryTaskStartOffset = 800;
    long recoveryTaskEndOffset = 1000;
    String topic = "foo";

    RecoveryService.PartitionOffsets offsets =
        RecoveryService.validateKafkaOffsets(
            getAdminClient(kafkaStartOffset, kafkaEndOffset),
            new RecoveryTaskMetadata("foo", "1", recoveryTaskStartOffset, recoveryTaskEndOffset, 1),
            topic);

    assertThat(offsets.startOffset).isEqualTo(recoveryTaskStartOffset);
    assertThat(offsets.endOffset).isEqualTo(kafkaEndOffset);
  }

  @Test
  public void shouldHandleInvalidRecoveryTasks() throws Exception {
    KaldbConfigs.KaldbConfig kaldbCfg = makeKaldbConfig(TEST_S3_BUCKET);
    curatorFramework =
        CuratorBuilder.build(meterRegistry, kaldbCfg.getMetadataStoreConfig().getZookeeperConfig());

    // Start recovery service
    recoveryService = new RecoveryService(kaldbCfg, curatorFramework, meterRegistry, blobFs);
    recoveryService.startAsync();
    recoveryService.awaitRunning(DEFAULT_START_STOP_DURATION);

    // Create a recovery task
    RecoveryTaskMetadataStore recoveryTaskMetadataStore =
        new RecoveryTaskMetadataStore(curatorFramework, false, meterRegistry);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).size()).isZero();
    RecoveryTaskMetadata recoveryTask =
        new RecoveryTaskMetadata("testRecoveryTask", "0", 0, 0, Instant.now().toEpochMilli());
    recoveryTaskMetadataStore.createSync(recoveryTask);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).size())
        .isEqualTo(1);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).get(0))
        .isEqualTo(recoveryTask);

    // Assign the recovery task to node.
    RecoveryNodeMetadataStore recoveryNodeMetadataStore =
        new RecoveryNodeMetadataStore(curatorFramework, false, meterRegistry);
    List<RecoveryNodeMetadata> recoveryNodes =
        KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore);
    assertThat(recoveryNodes.size()).isEqualTo(1);
    RecoveryNodeMetadata recoveryNodeMetadata = recoveryNodes.get(0);
    assertThat(recoveryNodeMetadata.recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);
    recoveryNodeMetadataStore.updateSync(
        new RecoveryNodeMetadata(
            recoveryNodeMetadata.getName(),
            Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED,
            recoveryTask.getName(),
            Instant.now().toEpochMilli()));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).size())
        .isEqualTo(1);

    await().until(() -> getCount(RECOVERY_NODE_ASSIGNMENT_FAILED, meterRegistry) == 1);
    assertThat(getCount(RECOVERY_NODE_ASSIGNMENT_SUCCESS, meterRegistry)).isZero();
    assertThat(getCount(RECOVERY_NODE_ASSIGNMENT_RECEIVED, meterRegistry)).isEqualTo(1);

    // Post recovery checks
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore).size())
        .isEqualTo(1);

    await()
        .until(
            () ->
                KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore)
                        .get(0)
                        .recoveryNodeState
                    == Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);

    recoveryNodeMetadataStore.close();
    recoveryTaskMetadataStore.close();
  }

  // returns startOffset or endOffset based on the supplied OffsetSpec
  private static AdminClient getAdminClient(long startOffset, long endOffset) {
    AdminClient adminClient = mock(AdminClient.class);
    org.mockito.Mockito.when(adminClient.listOffsets(anyMap()))
        .thenAnswer(
            (Answer<ListOffsetsResult>)
                invocation -> {
                  Map<TopicPartition, OffsetSpec> input = invocation.getArgument(0);
                  if (input.size() == 1) {
                    long value = -1;
                    OffsetSpec offsetSpec = input.values().stream().findFirst().get();
                    if (offsetSpec instanceof OffsetSpec.EarliestSpec) {
                      value = startOffset;
                    } else if (offsetSpec instanceof OffsetSpec.LatestSpec) {
                      value = endOffset;
                    } else {
                      throw new IllegalArgumentException("Invalid OffsetSpec supplied");
                    }
                    return new ListOffsetsResult(
                        Map.of(
                            input.keySet().stream().findFirst().get(),
                            KafkaFuture.completedFuture(
                                new ListOffsetsResult.ListOffsetsResultInfo(
                                    value, 0, Optional.of(0)))));
                  }
                  return null;
                });

    return adminClient;
  }
}
