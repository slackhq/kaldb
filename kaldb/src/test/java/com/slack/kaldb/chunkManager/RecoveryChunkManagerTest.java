package com.slack.kaldb.chunkManager;

import static com.slack.kaldb.chunk.ChunkInfo.MAX_FUTURE_TIME;
import static com.slack.kaldb.chunkManager.IndexingChunkManager.LIVE_BYTES_INDEXED;
import static com.slack.kaldb.chunkManager.IndexingChunkManager.LIVE_MESSAGES_INDEXED;
import static com.slack.kaldb.chunkManager.RollOverChunkTask.ROLLOVERS_COMPLETED;
import static com.slack.kaldb.chunkManager.RollOverChunkTask.ROLLOVERS_FAILED;
import static com.slack.kaldb.chunkManager.RollOverChunkTask.ROLLOVERS_INITIATED;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.server.KaldbConfig.CHUNK_DATA_PREFIX;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.ChunkManagerUtil.fetchLiveSnapshot;
import static com.slack.kaldb.testlib.ChunkManagerUtil.fetchNonLiveSnapshot;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.MetricsUtil.getValue;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherExtension.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.slack.kaldb.blobfs.s3.S3CrtBlobFs;
import com.slack.kaldb.blobfs.s3.S3TestUtils;
import com.slack.kaldb.chunk.ChunkInfo;
import com.slack.kaldb.chunk.ReadWriteChunk;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.core.KaldbMetadataTestUtils;
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.schema.Schema;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.SpanUtil;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherExtension;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class RecoveryChunkManagerTest {
  // TODO: Ensure clean close after all chunks are uploaded.
  // TODO: Test post snapshot for recovery.

  private static final String S3_TEST_BUCKET = "test-kaldb-logs";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(S3_TEST_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  private static final String TEST_KAFKA_PARTITION_ID = "10";

  private RecoveryChunkManager<LogMessage> chunkManager = null;

  private SimpleMeterRegistry metricsRegistry;
  private S3AsyncClient s3AsyncClient;

  private static final String ZK_PATH_PREFIX = "testZK";
  private S3CrtBlobFs s3CrtBlobFs;
  private TestingServer localZkServer;
  private AsyncCuratorFramework curatorFramework;
  private SearchMetadataStore searchMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;

  private KaldbConfigs.KaldbConfig kaldbConfig;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    // create an S3 client.
    s3AsyncClient = S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
    s3CrtBlobFs = new S3CrtBlobFs(s3AsyncClient);

    localZkServer = new TestingServer();
    localZkServer.start();

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(localZkServer.getConnectString())
            .setZkPathPrefix(ZK_PATH_PREFIX)
            .setZkSessionTimeoutMs(15000)
            .setZkConnectionTimeoutMs(1500)
            .setSleepBetweenRetriesMs(1000)
            .build();

    curatorFramework = CuratorBuilder.build(metricsRegistry, zkConfig);
    searchMetadataStore = new SearchMetadataStore(curatorFramework, false);
    snapshotMetadataStore = new SnapshotMetadataStore(curatorFramework);
  }

  @AfterEach
  public void tearDown() throws TimeoutException, IOException, InterruptedException {
    metricsRegistry.close();
    if (chunkManager != null) {
      chunkManager.stopAsync();
      chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    searchMetadataStore.close();
    snapshotMetadataStore.close();
    curatorFramework.unwrap().close();
    s3AsyncClient.close();
    localZkServer.stop();
  }

  private void initChunkManager(String testS3Bucket) throws Exception {

    kaldbConfig =
        KaldbConfigUtil.makeKaldbConfig(
            "localhost:9090",
            9000,
            "testKafkaTopic",
            0,
            "kaldb_test_client",
            testS3Bucket,
            9000 + 1,
            "localhost:2181",
            "recoveryZK_",
            KaldbConfigs.NodeRole.RECOVERY,
            10000,
            9003,
            100);

    chunkManager =
        RecoveryChunkManager.fromConfig(
            metricsRegistry,
            searchMetadataStore,
            snapshotMetadataStore,
            kaldbConfig.getIndexerConfig(),
            s3CrtBlobFs,
            kaldbConfig.getS3Config());

    chunkManager.startAsync();
    chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void testAddMessageAndRollover() throws Exception {
    initChunkManager(S3_TEST_BUCKET);

    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now());
    int actualChunkSize = 0;
    int offset = 1;
    for (Trace.Span m : messages) {
      final int msgSize = m.toString().length();
      chunkManager.addMessage(m, msgSize, TEST_KAFKA_PARTITION_ID, offset);
      actualChunkSize += msgSize;
      offset++;
    }
    ReadWriteChunk<LogMessage> currentChunk = chunkManager.getActiveChunk();
    currentChunk.commit();

    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(100);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(100);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(actualChunkSize);

    // Check metadata registration. No metadata registration until rollover.
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore)).isEmpty();

    // Search query
    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            "Message1",
            0,
            MAX_TIME,
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"),
            Collections.emptyList());
    SearchResult<LogMessage> results = chunkManager.getActiveChunk().query(searchQuery);
    assertThat(results.hits.size()).isEqualTo(1);

    // Test chunk metadata.
    ChunkInfo chunkInfo = chunkManager.getActiveChunk().info();
    assertThat(chunkInfo.getChunkSnapshotTimeEpochMs()).isZero();
    assertThat(chunkInfo.getDataStartTimeEpochMs()).isGreaterThan(0);
    assertThat(chunkInfo.getDataEndTimeEpochMs()).isGreaterThan(0);
    assertThat(chunkInfo.chunkId).startsWith(CHUNK_DATA_PREFIX);
    assertThat(chunkInfo.getMaxOffset()).isEqualTo(offset - 1);
    assertThat(chunkInfo.getDataStartTimeEpochMs())
        .isEqualTo(
            TimeUnit.MILLISECONDS.convert(messages.get(0).getTimestamp(), TimeUnit.MICROSECONDS));
    assertThat(chunkInfo.getDataEndTimeEpochMs())
        .isEqualTo(
            TimeUnit.MILLISECONDS.convert(messages.get(99).getTimestamp(), TimeUnit.MICROSECONDS));

    // Add a message with a very high offset.
    final int veryHighOffset = 1000;
    assertThat(chunkManager.getActiveChunk().info().getMaxOffset()).isEqualTo(offset - 1);
    assertThat(veryHighOffset - offset).isGreaterThan(100);
    Trace.Span messageWithHighOffset = SpanUtil.makeSpan(101);
    chunkManager.addMessage(
        messageWithHighOffset,
        messageWithHighOffset.toString().length(),
        TEST_KAFKA_PARTITION_ID,
        veryHighOffset);
    assertThat(chunkManager.getActiveChunk().info().getMaxOffset()).isEqualTo(veryHighOffset);
    chunkManager.getActiveChunk().commit();
    assertThat(
            chunkManager
                .getActiveChunk()
                .query(
                    new SearchQuery(
                        MessageUtil.TEST_DATASET_NAME,
                        "Message101",
                        0,
                        MAX_TIME,
                        10,
                        new DateHistogramAggBuilder(
                            "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"),
                        Collections.emptyList()))
                .hits
                .size())
        .isEqualTo(1);

    // Add a message with a lower offset.
    final int lowerOffset = 500;
    assertThat(chunkManager.getActiveChunk().info().getMaxOffset()).isEqualTo(veryHighOffset);
    assertThat(lowerOffset - offset).isGreaterThan(100);
    assertThat(veryHighOffset - lowerOffset).isGreaterThan(100);
    Trace.Span messageWithLowerOffset = SpanUtil.makeSpan(102);
    chunkManager.addMessage(
        messageWithLowerOffset,
        messageWithLowerOffset.toString().length(),
        TEST_KAFKA_PARTITION_ID,
        lowerOffset);
    assertThat(chunkManager.getActiveChunk().info().getMaxOffset()).isEqualTo(veryHighOffset);
    chunkManager.getActiveChunk().commit();
    assertThat(
            chunkManager
                .getActiveChunk()
                .query(
                    new SearchQuery(
                        MessageUtil.TEST_DATASET_NAME,
                        "Message102",
                        0,
                        MAX_TIME,
                        10,
                        new DateHistogramAggBuilder(
                            "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"),
                        Collections.emptyList()))
                .hits
                .size())
        .isEqualTo(1);

    // Inserting a message from a different kafka partition fails
    Trace.Span messageWithInvalidTopic = SpanUtil.makeSpan(103);
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                chunkManager.addMessage(
                    messageWithInvalidTopic,
                    messageWithInvalidTopic.toString().length(),
                    "differentKafkaTopic",
                    lowerOffset + 1));

    // Get the count of the amount of indices so that we can confirm we've cleaned them up
    // after the rollover
    final File dataDirectory = new File(kaldbConfig.getIndexerConfig().getDataDirectory());
    final File indexDirectory = new File(dataDirectory.getAbsolutePath() + "/indices");
    File[] filesBeforeRollover = indexDirectory.listFiles();
    assertThat(filesBeforeRollover).isNotNull();
    assertThat(filesBeforeRollover).isNotEmpty();

    // Roll over chunk.
    assertThat(chunkManager.waitForRollOvers()).isTrue();
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore)).isEmpty();
    List<SnapshotMetadata> snapshots =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshots.size()).isEqualTo(1);
    assertThat(snapshots.get(0).startTimeEpochMs)
        .isEqualTo(
            TimeUnit.MILLISECONDS.convert(messages.get(0).getTimestamp(), TimeUnit.MICROSECONDS));
    assertThat(snapshots.get(0).endTimeEpochMs)
        .isGreaterThanOrEqualTo(
            TimeUnit.MILLISECONDS.convert(messages.get(99).getTimestamp(), TimeUnit.MICROSECONDS));

    // Can't add messages to current chunk after roll over.
    assertThatThrownBy(
            () ->
                currentChunk.addMessage(SpanUtil.makeSpan(100000), TEST_KAFKA_PARTITION_ID, 100000))
        .isInstanceOf(IllegalStateException.class);

    // Ensure data is cleaned up in the manager
    assertThat(chunkManager.getChunkList()).isEmpty();
    assertThat(chunkManager.getActiveChunk()).isNull();

    // Ensure data on disk is deleted.
    File[] filesAfterRollover = indexDirectory.listFiles();
    assertThat(filesAfterRollover).isNotNull();
    assertThat(filesBeforeRollover.length > filesAfterRollover.length).isTrue();

    chunkManager.stopAsync();
    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    chunkManager = null;
  }

  private void testChunkManagerSearch(
      ChunkManager<LogMessage> chunkManager, String searchString, int expectedHitCount) {

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            searchString,
            0,
            TemporaryLogStoreAndSearcherExtension.MAX_TIME,
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"),
            Collections.emptyList());
    SearchResult<LogMessage> result = chunkManager.query(searchQuery, Duration.ofMillis(3000));

    assertThat(result.hits.size()).isEqualTo(expectedHitCount);

    // Special case: if we're expecting this search to have no hits then it won't have any
    // snapshots
    // or replicas either
    if (expectedHitCount == 0) {
      assertThat(result.totalSnapshots).isEqualTo(0);
      assertThat(result.snapshotsWithReplicas).isEqualTo(0);
    } else {
      assertThat(result.totalSnapshots).isEqualTo(1);
      assertThat(result.snapshotsWithReplicas).isEqualTo(1);
    }
  }

  // TODO: Add a unit test where the chunk manager uses a different field conflict policy like
  // RAISE_ERROR.

  @Test
  public void testAddMessageWithPropertyTypeConflicts() throws Exception {
    initChunkManager(S3_TEST_BUCKET);

    // Add a valid message
    int offset = 1;
    Trace.Span msg1 = SpanUtil.makeSpan(1);
    chunkManager.addMessage(msg1, msg1.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
    offset++;

    // Add an invalid message
    Trace.KeyValue conflictTag =
        Trace.KeyValue.newBuilder()
            .setVInt32(20000)
            .setKey(LogMessage.ReservedField.HOSTNAME.fieldName)
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();
    Trace.Span msg100 = SpanUtil.makeSpan(100, "Message100", Instant.now(), List.of(conflictTag));
    chunkManager.addMessage(msg100, msg100.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
    //noinspection UnusedAssignment
    offset++;

    // Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();

    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    testChunkManagerSearch(chunkManager, "Message1", 1);
    testChunkManagerSearch(chunkManager, "Message100", 1);

    // Check metadata.
    List<SnapshotMetadata> snapshots =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshots.size()).isEqualTo(0);
    List<SnapshotMetadata> liveSnapshots = fetchLiveSnapshot(snapshots);
    assertThat(liveSnapshots.size()).isZero();
    assertThat(fetchNonLiveSnapshot(snapshots).size()).isEqualTo(0);
    List<SearchMetadata> searchNodes = KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore);
    assertThat(searchNodes).isEmpty();
    assertThat(liveSnapshots.stream().map(s -> s.snapshotId).collect(Collectors.toList()))
        .isEmpty();
    assertThat(snapshots.stream().filter(s -> s.endTimeEpochMs == MAX_FUTURE_TIME)).isEmpty();
  }

  // TODO: Add a test to create roll over failure due to ZK.

  @Test
  public void testAddMessagesWithFailedRollOverStopsIngestion() throws Exception {
    // Use a non-existent bucket to induce roll-over failure.
    initChunkManager("fakebucket");

    int offset = 1;

    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 20, 1, Instant.now());
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    assertThat(chunkManager.waitForRollOvers()).isFalse();

    assertThat(chunkManager.getChunkList().size()).isEqualTo(0);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(20);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(1);
    testChunkManagerSearch(chunkManager, "Message1", 0);

    // Ensure can't add messages once roll over is complete.
    assertThatIllegalStateException()
        .isThrownBy(
            () ->
                chunkManager.addMessage(
                    SpanUtil.makeSpan(1000), 100, TEST_KAFKA_PARTITION_ID, 1000));

    // Check metadata.
    List<SnapshotMetadata> snapshots =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshots.size()).isEqualTo(0);
    List<SnapshotMetadata> liveSnapshots = fetchLiveSnapshot(snapshots);
    assertThat(liveSnapshots.size()).isZero();
    assertThat(fetchNonLiveSnapshot(snapshots).size()).isEqualTo(0);
    List<SearchMetadata> searchNodes = KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore);
    assertThat(searchNodes).isEmpty();
    assertThat(liveSnapshots.stream().map(s -> s.snapshotId).collect(Collectors.toList()))
        .isEmpty();
    assertThat(snapshots.stream().filter(s -> s.endTimeEpochMs == MAX_FUTURE_TIME)).isEmpty();

    // roll over active chunk on close.
    chunkManager.stopAsync();

    // Can't add messages to a chunk that's shutdown.
    assertThatIllegalStateException()
        .isThrownBy(
            () ->
                chunkManager.addMessage(
                    SpanUtil.makeSpan(1000), 100, TEST_KAFKA_PARTITION_ID, 1000));

    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    chunkManager = null;
  }
}
