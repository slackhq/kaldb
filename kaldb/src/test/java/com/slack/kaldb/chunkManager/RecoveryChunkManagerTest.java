package com.slack.kaldb.chunkManager;

import static com.slack.kaldb.chunk.ChunkInfo.MAX_FUTURE_TIME;
import static com.slack.kaldb.chunkManager.IndexingChunkManager.LIVE_BYTES_INDEXED;
import static com.slack.kaldb.chunkManager.IndexingChunkManager.LIVE_MESSAGES_INDEXED;
import static com.slack.kaldb.chunkManager.RollOverChunkTask.*;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.ChunkManagerUtil.*;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.MetricsUtil.getValue;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule.MAX_TIME;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunk.*;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.curator.test.TestingServer;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class RecoveryChunkManagerTest {
  // TODO: test for parallel ingestion failure in indexer.
  // TODO: Ensure clean close after all chunks are uploaded.
  // TODO: Test post snapshot for recovery.
  // TODO: Add a test for roll over failed. *
  // TODO: Add rollover and close logic to all the unit tests. **

  private static final Logger LOG = LoggerFactory.getLogger(RecoveryChunkManagerTest.class);

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private static final String TEST_KAFKA_PARTITION_ID = "10";
  private static final String TEST_CHUNK_DATA_PREFIX = "testData";
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private RecoveryChunkManager<LogMessage> chunkManager = null;

  private SimpleMeterRegistry metricsRegistry;
  private S3Client s3Client;

  private static final String S3_TEST_BUCKET = "test-kaldb-logs";
  private static final String ZK_PATH_PREFIX = "testZK";
  private S3BlobFs s3BlobFs;
  private TestingServer localZkServer;
  private MetadataStore metadataStore;
  private SearchMetadataStore searchMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;

  @Before
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    // create an S3 client.
    s3Client = S3_MOCK_RULE.createS3ClientV2();
    s3Client.createBucket(CreateBucketRequest.builder().bucket(S3_TEST_BUCKET).build());
    s3BlobFs = new S3BlobFs(s3Client);

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

    metadataStore = ZookeeperMetadataStoreImpl.fromConfig(metricsRegistry, zkConfig);
    searchMetadataStore = new SearchMetadataStore(metadataStore, false);
    snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, false);
  }

  @After
  public void tearDown() throws TimeoutException, IOException, InterruptedException {
    metricsRegistry.close();
    if (chunkManager != null) {
      chunkManager.stopAsync();
      chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    searchMetadataStore.close();
    snapshotMetadataStore.close();
    metadataStore.close();
    s3Client.close();
    localZkServer.stop();
  }

  private void initChunkManager(ChunkRollOverStrategy chunkRollOverStrategy, String s3TestBucket)
      throws TimeoutException {
    SearchContext searchContext = new SearchContext(TEST_HOST, TEST_PORT);

    // KaldbConfigUtil.makeIndexerConfig(TEST_PORT, 1000, "log_message", 100));
    KaldbConfigs.IndexerConfig indexerConfig = KaldbConfigUtil.makeIndexerConfig();
    ChunkFactory<LogMessage> chunkFactory =
        new RecoveryChunkFactoryImpl<>(
            indexerConfig,
            "testData",
            metricsRegistry,
            searchMetadataStore,
            snapshotMetadataStore,
            searchContext);
    ChunkRolloverFactory chunkRolloverFactory =
        new ChunkRolloverFactory(chunkRollOverStrategy, s3BlobFs, s3TestBucket, metricsRegistry);
    chunkManager = new RecoveryChunkManager<>(chunkFactory, chunkRolloverFactory, metricsRegistry);
    chunkManager.startAsync();
    chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void testAddMessage() throws Exception {
    final Instant creationTime = Instant.now();
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 1000000L);

    initChunkManager(chunkRollOverStrategy, S3_TEST_BUCKET);

    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
    int actualChunkSize = 0;
    int offset = 1;
    for (LogMessage m : messages) {
      final int msgSize = m.toString().length();
      chunkManager.addMessage(m, msgSize, TEST_KAFKA_PARTITION_ID, offset);
      actualChunkSize += msgSize;
      offset++;
    }
    chunkManager.getActiveChunk().commit();

    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(100);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(100);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(actualChunkSize);

    // Check metadata registration. No metadata registration until rollover.
    assertThat(snapshotMetadataStore.listSync()).isEmpty();
    assertThat(searchMetadataStore.listSync()).isEmpty();

    // Search query
    SearchQuery searchQuery =
        new SearchQuery(MessageUtil.TEST_INDEX_NAME, "Message1", 0, MAX_TIME, 10, 1000);
    SearchResult<LogMessage> results = chunkManager.getActiveChunk().query(searchQuery);
    assertThat(results.hits.size()).isEqualTo(1);

    // Test chunk metadata.
    ChunkInfo chunkInfo = chunkManager.getActiveChunk().info();
    assertThat(chunkInfo.getChunkSnapshotTimeEpochMs()).isZero();
    assertThat(chunkInfo.getDataStartTimeEpochMs()).isGreaterThan(0);
    assertThat(chunkInfo.getDataEndTimeEpochMs()).isGreaterThan(0);
    assertThat(chunkInfo.chunkId).startsWith(TEST_CHUNK_DATA_PREFIX);
    assertThat(chunkInfo.getMaxOffset()).isEqualTo(offset - 1);
    assertThat(chunkInfo.getDataStartTimeEpochMs()).isEqualTo(messages.get(0).timeSinceEpochMilli);
    assertThat(chunkInfo.getDataEndTimeEpochMs()).isEqualTo(messages.get(99).timeSinceEpochMilli);

    // Add a message with a very high offset.
    final int veryHighOffset = 1000;
    assertThat(chunkManager.getActiveChunk().info().getMaxOffset()).isEqualTo(offset - 1);
    assertThat(veryHighOffset - offset).isGreaterThan(100);
    LogMessage messageWithHighOffset = MessageUtil.makeMessage(101);
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
                        MessageUtil.TEST_INDEX_NAME, "Message101", 0, MAX_TIME, 10, 1000))
                .hits
                .size())
        .isEqualTo(1);

    // Add a message with a lower offset.
    final int lowerOffset = 500;
    assertThat(chunkManager.getActiveChunk().info().getMaxOffset()).isEqualTo(veryHighOffset);
    assertThat(lowerOffset - offset).isGreaterThan(100);
    assertThat(veryHighOffset - lowerOffset).isGreaterThan(100);
    LogMessage messageWithLowerOffset = MessageUtil.makeMessage(102);
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
                        MessageUtil.TEST_INDEX_NAME, "Message102", 0, MAX_TIME, 10, 1000))
                .hits
                .size())
        .isEqualTo(1);

    // Inserting a message from a different kafka partition fails
    LogMessage messageWithInvalidTopic = MessageUtil.makeMessage(103);
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                chunkManager.addMessage(
                    messageWithInvalidTopic,
                    messageWithInvalidTopic.toString().length(),
                    "differentKafkaTopic",
                    lowerOffset + 1));
  }

  private void testChunkManagerSearch(
      ChunkManager<LogMessage> chunkManager,
      String searchString,
      int expectedHitCount,
      int totalSnapshots,
      int expectedSnapshotsWithReplicas,
      long startTimeEpochMs,
      long endTimeEpochMs) {

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_INDEX_NAME, searchString, startTimeEpochMs, endTimeEpochMs, 10, 1000);
    SearchResult<LogMessage> result = chunkManager.query(searchQuery);

    assertThat(result.hits.size()).isEqualTo(expectedHitCount);
    assertThat(result.totalSnapshots).isEqualTo(totalSnapshots);
    assertThat(result.snapshotsWithReplicas).isEqualTo(expectedSnapshotsWithReplicas);
  }

  private void checkMetadata(int expectedSnapshotSize, int expectedNonLiveSnapshotSize) {
    List<SnapshotMetadata> snapshots = snapshotMetadataStore.listSync();
    assertThat(snapshots.size()).isEqualTo(expectedSnapshotSize);
    List<SnapshotMetadata> liveSnapshots = fetchLiveSnapshot(snapshots);
    assertThat(liveSnapshots.size()).isZero();
    assertThat(fetchNonLiveSnapshot(snapshots).size()).isEqualTo(expectedNonLiveSnapshotSize);
    List<SearchMetadata> searchNodes = searchMetadataStore.listSync();
    assertThat(searchNodes).isEmpty();
    assertThat(liveSnapshots.stream().map(s -> s.snapshotId).collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(
            searchNodes.stream().map(s -> s.snapshotName).collect(Collectors.toList()));
    assertThat(snapshots.stream().filter(s -> s.endTimeEpochMs == MAX_FUTURE_TIME)).isEmpty();
  }

  @Test
  public void testAddAndSearchMessageInMultipleSlices() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);

    initChunkManager(chunkRollOverStrategy, S3_TEST_BUCKET);

    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 15);
    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    chunkManager.getActiveChunk().commit();

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(15);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);

    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message11", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message1 OR Message11", 2, 2, 2, 0, MAX_TIME);

    checkMetadata(1, 1);
  }

  @Test
  public void testAddMessageWithPropertyTypeErrors() throws IOException, TimeoutException {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);

    initChunkManager(chunkRollOverStrategy, S3_TEST_BUCKET);

    // Add a valid message
    int offset = 1;
    LogMessage msg1 = MessageUtil.makeMessage(1);
    chunkManager.addMessage(msg1, msg1.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
    offset++;

    // Add an invalid message
    LogMessage msg100 = MessageUtil.makeMessage(100);
    MessageUtil.addFieldToMessage(msg100, LogMessage.ReservedField.HOSTNAME.fieldName, 20000);
    chunkManager.addMessage(msg100, msg100.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
    offset++;

    // Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();

    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(1);
    testChunkManagerSearch(chunkManager, "Message1", 1, 1, 1, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message100", 0, 1, 1, 0, MAX_TIME);

    // Check metadata.
    checkMetadata(0, 0);
  }

  @Test(expected = IllegalStateException.class)
  public void testMessagesAddedToInactiveChunks() throws IOException, TimeoutException {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 2L);

    initChunkManager(chunkRollOverStrategy, S3_TEST_BUCKET);

    // Add a message
    List<LogMessage> msgs = MessageUtil.makeMessagesWithTimeDifference(1, 4, 1000);
    LogMessage msg1 = msgs.get(0);
    LogMessage msg2 = msgs.get(1);
    int offset = 1;
    chunkManager.addMessage(msg1, msg1.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
    offset++;
    ReadWriteChunk<LogMessage> chunk1 = chunkManager.getActiveChunk();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(1);

    chunkManager.addMessage(msg2, msg2.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
    offset++;
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(0); // Roll over.
    // Wait for roll over to complete without using an external mechanism.
    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 1);

    testChunkManagerSearch(chunkManager, "Message2", 1, 1, 1, 0, MAX_TIME);
    checkMetadata(1, 1);

    LogMessage msg3 = msgs.get(2);
    LogMessage msg4 = msgs.get(3);
    chunkManager.addMessage(msg3, msg3.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
    offset++;
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(1);
    // Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();
    testChunkManagerSearch(chunkManager, "Message3", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2, 0, MAX_TIME);

    checkMetadata(1, 1);
    // Inserting in an older chunk throws an exception. So, additions go to active chunks only.
    chunk1.addMessage(msg4, TEST_KAFKA_PARTITION_ID, 1);
  }

  @Test
  public void testAddMessagesWithTwoParallelRollover() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);

    initChunkManager(chunkRollOverStrategy, S3_TEST_BUCKET);

    int offset = 1;
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 20);
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    assertThat(chunkManager.waitForRollOvers()).isTrue();

    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(20);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    await().until(() -> getCount(ROLLOVERS_INITIATED, metricsRegistry) == 2);
    await().until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 2);
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message20", 1, 2, 2, 0, MAX_TIME);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);

    // Ensure can't add messages once roll over is complete.
    assertThatIllegalStateException()
        .isThrownBy(
            () ->
                chunkManager.addMessage(
                    MessageUtil.makeMessage(1000), 100, TEST_KAFKA_PARTITION_ID, 1000));

    // Check metadata.
    checkMetadata(2, 2);

    // roll over active chunk on close.
    chunkManager.stopAsync();

    // Can't add messages to a chunk that's shutdown.
    assertThatIllegalStateException()
        .isThrownBy(
            () ->
                chunkManager.addMessage(
                    MessageUtil.makeMessage(1000), 100, TEST_KAFKA_PARTITION_ID, 1000));

    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    chunkManager = null;
  }

  @Test
  public void testAddMessagesWithTenParallelRollover() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);

    initChunkManager(chunkRollOverStrategy, S3_TEST_BUCKET);

    int offset = 1;
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 101);
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    assertThat(chunkManager.waitForRollOvers()).isTrue();

    assertThat(chunkManager.getChunkList().size()).isEqualTo(11);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(101);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    await().until(() -> getCount(ROLLOVERS_INITIATED, metricsRegistry) == 11);
    await().until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 11);
    testChunkManagerSearch(chunkManager, "Message1", 1, 11, 11, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message101", 1, 11, 11, 0, MAX_TIME);

    // Check metadata.
    checkMetadata(11, 11);

    // Ensure can't add messages once roll over is complete.
    assertThatIllegalStateException()
        .isThrownBy(
            () ->
                chunkManager.addMessage(
                    MessageUtil.makeMessage(1000), 100, TEST_KAFKA_PARTITION_ID, 1000));

    // roll over active chunk on close.
    chunkManager.stopAsync();

    // Can't add messages to a chunk that's shutdown.
    assertThatIllegalStateException()
        .isThrownBy(
            () ->
                chunkManager.addMessage(
                    MessageUtil.makeMessage(1000), 100, TEST_KAFKA_PARTITION_ID, 1000));

    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    chunkManager = null;
  }

  @Test
  public void testCommitInvalidChunk() throws Exception {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

    final List<LogMessage> messages =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000, startTime);
    messages.addAll(
        MessageUtil.makeMessagesWithTimeDifference(
            11, 20, 1000, startTime.plus(2, ChronoUnit.HOURS)));
    messages.addAll(
        MessageUtil.makeMessagesWithTimeDifference(
            21, 30, 1000, startTime.plus(4, ChronoUnit.HOURS)));

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);
    initChunkManager(chunkRollOverStrategy, S3_TEST_BUCKET);

    int offset = 1;
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 3);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(3);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(30);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    checkMetadata(3, 3);

    // No new chunk left to commit.
    assertThat(chunkManager.getActiveChunk()).isNull();
  }

  // TODO: Add a test to create roll over failure due to ZK.

  @Test
  public void testAddMessagesWithFailedRollOver() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);

    // Use a non-existent bucket to induce roll-over failure.
    initChunkManager(chunkRollOverStrategy, "fakebucket");

    int offset = 1;
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 20);
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    assertThat(chunkManager.waitForRollOvers()).isFalse();

    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(20);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry) == 2);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry) == 1);
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2, 0, MAX_TIME);
    testChunkManagerSearch(chunkManager, "Message20", 1, 2, 2, 0, MAX_TIME);

    // Ensure can't add messages once roll over is complete.
    assertThatIllegalStateException()
        .isThrownBy(
            () ->
                chunkManager.addMessage(
                    MessageUtil.makeMessage(1000), 100, TEST_KAFKA_PARTITION_ID, 1000));

    // Check metadata.
    checkMetadata(0, 0);

    // roll over active chunk on close.
    chunkManager.stopAsync();

    // Can't add messages to a chunk that's shutdown.
    assertThatIllegalStateException()
        .isThrownBy(
            () ->
                chunkManager.addMessage(
                    MessageUtil.makeMessage(1000), 100, TEST_KAFKA_PARTITION_ID, 1000));

    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    chunkManager = null;
  }

  @Test(expected = IllegalStateException.class)
  public void testFailedRolloverStopsIngestion() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new ChunkRollOverStrategyImpl(10 * 1024 * 1024 * 1024L, 10L);

    // Use a non-existent bucket to induce roll-over failure.
    initChunkManager(chunkRollOverStrategy, "fakebucket");

    int offset = 1;
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      if (offset % 10 == 0) {
        Thread.sleep(1000);
      }
      offset++;
    }
  }
}