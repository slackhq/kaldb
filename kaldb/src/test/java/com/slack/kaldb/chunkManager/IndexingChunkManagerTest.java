package com.slack.kaldb.chunkManager;

import static com.slack.kaldb.chunk.ChunkInfo.MAX_FUTURE_TIME;
import static com.slack.kaldb.chunkManager.IndexingChunkManager.LIVE_BYTES_INDEXED;
import static com.slack.kaldb.chunkManager.IndexingChunkManager.LIVE_MESSAGES_INDEXED;
import static com.slack.kaldb.chunkManager.RollOverChunkTask.ROLLOVERS_COMPLETED;
import static com.slack.kaldb.chunkManager.RollOverChunkTask.ROLLOVERS_FAILED;
import static com.slack.kaldb.chunkManager.RollOverChunkTask.ROLLOVERS_INITIATED;
import static com.slack.kaldb.chunkManager.RollOverChunkTask.ROLLOVER_TIMER;
import static com.slack.kaldb.chunkrollover.DiskOrMessageCountBasedRolloverStrategy.LIVE_BYTES_DIR;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.ChunkManagerUtil.TEST_HOST;
import static com.slack.kaldb.testlib.ChunkManagerUtil.TEST_PORT;
import static com.slack.kaldb.testlib.ChunkManagerUtil.fetchLiveSnapshot;
import static com.slack.kaldb.testlib.ChunkManagerUtil.fetchNonLiveSnapshot;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.MetricsUtil.getTimerCount;
import static com.slack.kaldb.testlib.MetricsUtil.getValue;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherExtension.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.slack.kaldb.blobfs.s3.S3CrtBlobFs;
import com.slack.kaldb.blobfs.s3.S3TestUtils;
import com.slack.kaldb.chunk.Chunk;
import com.slack.kaldb.chunk.ChunkInfo;
import com.slack.kaldb.chunk.ReadWriteChunk;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.chunkrollover.ChunkRollOverStrategy;
import com.slack.kaldb.chunkrollover.DiskOrMessageCountBasedRolloverStrategy;
import com.slack.kaldb.chunkrollover.MessageSizeOrCountBasedRolloverStrategy;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.AlreadyClosedLogIndexSearcherImpl;
import com.slack.kaldb.logstore.search.IllegalArgumentLogIndexSearcherImpl;
import com.slack.kaldb.logstore.search.KaldbLocalQueryService;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.core.KaldbMetadataTestUtils;
import com.slack.kaldb.metadata.schema.FieldType;
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.schema.Schema;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.SpanUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class IndexingChunkManagerTest {

  private static final String S3_TEST_BUCKET = "test-kaldb-logs";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(S3_TEST_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  private static final String TEST_KAFKA_PARTITION_ID = "10";
  @TempDir private Path tmpPath;

  private IndexingChunkManager<LogMessage> chunkManager = null;

  private SimpleMeterRegistry metricsRegistry;
  private S3AsyncClient s3AsyncClient;

  private static final String ZK_PATH_PREFIX = "testZK";
  private S3CrtBlobFs s3CrtBlobFs;
  private TestingServer localZkServer;
  private AsyncCuratorFramework curatorFramework;
  private SnapshotMetadataStore snapshotMetadataStore;
  private SearchMetadataStore searchMetadataStore;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    // create an S3 client and a bucket for test
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
    snapshotMetadataStore = new SnapshotMetadataStore(curatorFramework);
    searchMetadataStore = new SearchMetadataStore(curatorFramework, false);
  }

  @AfterEach
  public void tearDown() throws TimeoutException, IOException {
    metricsRegistry.close();
    if (chunkManager != null) {
      chunkManager.stopAsync();
      chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    curatorFramework.unwrap().close();
    s3AsyncClient.close();
    localZkServer.stop();
  }

  private void initChunkManager(
      ChunkRollOverStrategy chunkRollOverStrategy,
      String s3TestBucket,
      ListeningExecutorService listeningExecutorService)
      throws IOException, TimeoutException {
    SearchContext searchContext = new SearchContext(TEST_HOST, TEST_PORT);
    chunkManager =
        new IndexingChunkManager<>(
            "testData",
            tmpPath.toFile().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3CrtBlobFs,
            s3TestBucket,
            listeningExecutorService,
            curatorFramework,
            searchContext,
            KaldbConfigUtil.makeIndexerConfig(TEST_PORT, 1000, 100));
    chunkManager.startAsync();
    chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  private void initChunkManager(
      ChunkRollOverStrategy chunkRollOverStrategy,
      String s3TestBucket,
      ListeningExecutorService listeningExecutorService,
      KaldbConfigs.IndexerConfig indexerConfig)
      throws IOException, TimeoutException {
    SearchContext searchContext = new SearchContext(TEST_HOST, TEST_PORT);
    chunkManager =
        new IndexingChunkManager<>(
            "testData",
            tmpPath.toFile().getAbsolutePath(),
            chunkRollOverStrategy,
            metricsRegistry,
            s3CrtBlobFs,
            s3TestBucket,
            listeningExecutorService,
            curatorFramework,
            searchContext,
            indexerConfig);
    chunkManager.startAsync();
    chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void testDeleteOverMaxThresholdGreaterThanZero() throws IOException, TimeoutException {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(metricsRegistry, 10 * 1024 * 1024 * 1024L, 10L);

    KaldbConfigs.IndexerConfig indexerConfig =
        KaldbConfigUtil.makeIndexerConfig(TEST_PORT, 1000, 100, 1, 1_000_000_000L);
    initChunkManager(
        chunkRollOverStrategy,
        S3_TEST_BUCKET,
        MoreExecutors.newDirectExecutorService(),
        indexerConfig);

    assertThat(chunkManager.getChunkList().isEmpty()).isTrue();
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 11, 1000, startTime);

    int offset = 1;
    for (Trace.Span m : messages.subList(0, 9)) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(9);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);

    final ReadWriteChunk<LogMessage> chunk1 = chunkManager.getActiveChunk();
    assertThat(chunk1.isReadOnly()).isFalse();
    assertThat(chunk1.info().getChunkSnapshotTimeEpochMs()).isZero();

    for (Trace.Span m : messages.subList(9, 11)) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    await().until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(11);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);

    checkMetadata(3, 2, 1, 2, 1);

    final ReadWriteChunk<LogMessage> chunk2 = chunkManager.getActiveChunk();
    assertThat(chunk1.isReadOnly()).isTrue();
    assertThat(chunk1.info().getChunkSnapshotTimeEpochMs()).isNotZero();
    assertThat(chunk2.isReadOnly()).isFalse();
    assertThat(chunk2.info().getChunkSnapshotTimeEpochMs()).isZero();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(11);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);

    assertThat(chunk1.isReadOnly()).isTrue();
    assertThat(chunk1.info().getChunkSnapshotTimeEpochMs()).isNotZero();

    // Confirm that we deleted chunk1 instead of chunk2, as chunk1 is the older chunk
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);

    // Commit the chunk1 and roll it over.
    chunkManager.rollOverActiveChunk();
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(chunkManager.getChunkList().contains(chunk1)).isFalse();
    assertThat(chunkManager.getChunkList().contains(chunk2)).isTrue();
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(2);
    checkMetadata(3, 1, 2, 1, 0);
    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).stream()
                .map(s -> s.maxOffset)
                .sorted()
                .collect(Collectors.toList()))
        .containsOnly(10L, 11L, 11L);
  }

  @Test
  public void testDeleteStaleDataDoesNothingWhenGivenLimitLessThan0()
      throws IOException, TimeoutException {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(
            metricsRegistry, 10 * 1024 * 1024 * 1024L, 1000000L);

    KaldbConfigs.IndexerConfig indexerConfig =
        KaldbConfigUtil.makeIndexerConfig(TEST_PORT, 1000, 100, -1, 10_000);
    initChunkManager(
        chunkRollOverStrategy,
        S3_TEST_BUCKET,
        MoreExecutors.newDirectExecutorService(),
        indexerConfig);

    assertThat(chunkManager.getChunkList().isEmpty()).isTrue();
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 11, 1000, startTime);

    int offset = 1;
    for (Trace.Span m : messages.subList(0, 9)) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(9);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);

    final ReadWriteChunk<LogMessage> chunk1 = chunkManager.getActiveChunk();
    assertThat(chunk1.isReadOnly()).isFalse();
    assertThat(chunk1.info().getChunkSnapshotTimeEpochMs()).isZero();

    // Get the count of the amount of indices so that we can confirm we've cleaned them up
    // after the rollover
    final File indexDirectory = tmpPath.resolve("indices").toFile();

    // files before rollover may or may-not be null, depending on other test timing
    int filesBeforeRollover =
        Optional.ofNullable(indexDirectory.listFiles()).orElse(new File[] {}).length;
    chunkManager.rollOverActiveChunk();

    // Ensure data on disk is NOT deleted.
    File[] filesAfterRollover = indexDirectory.listFiles();
    assertThat(filesAfterRollover).isNotNull();
    assertThat(filesBeforeRollover == filesAfterRollover.length).isTrue();
  }

  @Test
  @Disabled
  // Todo: this test needs to be refactored as it currently does not reliably replicate the race
  //   condition Additionally, this test as currently written is extremely slow, and accounts
  //   for over 10% of our test runtime
  public void closeDuringCleanerTask()
      throws IOException, TimeoutException, ExecutionException, InterruptedException {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new MessageSizeOrCountBasedRolloverStrategy(
            metricsRegistry, 10 * 1024 * 1024 * 1024L, 1000000L);

    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService());

    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 11, 1, Instant.now());
    int offset = 1;
    for (Trace.Span m : messages) {
      final int msgSize = m.toString().length();
      chunkManager.addMessage(m, msgSize, TEST_KAFKA_PARTITION_ID, offset);
      offset++;
      chunkManager.getActiveChunk().commit();

      // force creation of a unique chunk for every message
      chunkManager.rollOverActiveChunk();
    }
    assertThat(chunkManager.getChunkList().size()).isEqualTo(10);

    // attempt to clean all chunks while shutting the service down
    // we use an executor service since the chunkCleaner is an AbstractScheduledService and we
    // want
    // these to run immediately
    //    ExecutorService executorService = Executors.newSingleThreadExecutor();
    // NOTE: This doesn't make much sense anymore as this class/method has been removed. This is
    // only
    // left here for historical breadcrumbs
    //    Future<?> cleanerTask =
    //        executorService.submit(() -> chunkCleaner.deleteStaleData(Instant.now()));

    chunkManager.stopAsync();
    // wait for both to be complete
    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    //    cleanerTask.get(10, TimeUnit.SECONDS);

    assertThat(chunkManager.getChunkList().size()).isEqualTo(0);
  }

  @Test
  public void testAddMessage() throws Exception {
    final Instant creationTime = Instant.now();
    ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(
            metricsRegistry, 10 * 1024 * 1024 * 1024L, 1000000L);

    final String CHUNK_DATA_PREFIX = "testData";
    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService());

    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now());
    int actualChunkSize = 0;
    int offset = 1;
    for (Trace.Span m : messages) {
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

    // Check metadata registration.

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isEqualTo(1);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size()).isEqualTo(1);

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
    SearchResult<LogMessage> results = chunkManager.query(searchQuery, Duration.ofMillis(3000));
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

    List<SnapshotMetadata> snapshots =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshots.size()).isEqualTo(1);
    List<SnapshotMetadata> liveSnapshots = fetchLiveSnapshot(snapshots);
    assertThat(liveSnapshots.size()).isEqualTo(1);
    assertThat(fetchNonLiveSnapshot(snapshots)).isEmpty();
    assertThat(snapshots.get(0).snapshotPath).startsWith(SnapshotMetadata.LIVE_SNAPSHOT_PATH);
    assertThat(snapshots.get(0).maxOffset).isEqualTo(0);
    assertThat(snapshots.get(0).partitionId).isEqualTo(TEST_KAFKA_PARTITION_ID);
    assertThat(snapshots.get(0).snapshotId).startsWith(SnapshotMetadata.LIVE_SNAPSHOT_PATH);
    assertThat(snapshots.get(0).startTimeEpochMs)
        .isCloseTo(creationTime.toEpochMilli(), Offset.offset(5000L));
    assertThat(snapshots.get(0).endTimeEpochMs).isEqualTo(MAX_FUTURE_TIME);

    List<SearchMetadata> searchNodes = KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.get(0).url).contains(TEST_HOST);
    assertThat(searchNodes.get(0).url).contains(String.valueOf(TEST_PORT));
    assertThat(searchNodes.get(0).snapshotName).contains(SnapshotMetadata.LIVE_SNAPSHOT_PATH);

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
                .query(
                    new SearchQuery(
                        MessageUtil.TEST_DATASET_NAME,
                        "Message101",
                        0,
                        MAX_TIME,
                        10,
                        new DateHistogramAggBuilder(
                            "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"),
                        Collections.emptyList()),
                    Duration.ofMillis(3000))
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
                .query(
                    new SearchQuery(
                        MessageUtil.TEST_DATASET_NAME,
                        "Message102",
                        0,
                        MAX_TIME,
                        10,
                        new DateHistogramAggBuilder(
                            "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"),
                        Collections.emptyList()),
                    Duration.ofMillis(3000))
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
  }

  private void testChunkManagerSearch(
      ChunkManager<LogMessage> chunkManager,
      List<String> chunkIds,
      String searchString,
      int expectedHitCount,
      int totalSnapshots,
      int expectedSnapshotsWithReplicas) {

    KaldbLocalQueryService<LogMessage> kaldbLocalQueryService =
        new KaldbLocalQueryService<>(chunkManager, Duration.ofSeconds(3));
    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();
    KaldbSearch.SearchResult response =
        kaldbLocalQueryService.doSearch(
            searchRequestBuilder
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setQueryString(searchString)
                .setStartTimeEpochMs(0)
                .setEndTimeEpochMs(Long.MAX_VALUE)
                .setHowMany(10)
                .setAggregations(
                    KaldbSearch.SearchRequest.SearchAggregation.newBuilder()
                        .setType(DateHistogramAggBuilder.TYPE)
                        .setName("1")
                        .setValueSource(
                            KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                .newBuilder()
                                .setField(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName)
                                .setDateHistogram(
                                    KaldbSearch.SearchRequest.SearchAggregation
                                        .ValueSourceAggregation.DateHistogramAggregation
                                        .newBuilder()
                                        .setMinDocCount(1)
                                        .setInterval("1s")
                                        .build())
                                .build())
                        .build())
                .addAllChunkIds(chunkIds)
                .build());

    assertThat(response.getHitsList().size()).isEqualTo(expectedHitCount);
    assertThat(response.getTotalSnapshots()).isEqualTo(totalSnapshots);
    assertThat(response.getSnapshotsWithReplicas()).isEqualTo(expectedSnapshotsWithReplicas);
    assertThat(response.getFailedNodes()).isEqualTo(0);
    assertThat(response.getTotalNodes()).isEqualTo(1);
  }

  private void testChunkManagerSearch(
      ChunkManager<LogMessage> chunkManager,
      String searchString,
      int expectedHitCount,
      int totalSnapshots,
      int expectedSnapshotsWithReplicas) {

    testChunkManagerSearch(
        chunkManager,
        Collections.emptyList(),
        searchString,
        expectedHitCount,
        totalSnapshots,
        expectedSnapshotsWithReplicas);
  }

  private int searchAndGetHitCount(
      ChunkManager<LogMessage> chunkManager,
      String searchString,
      long startTimeEpochMs,
      long endTimeEpochMs) {
    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            searchString,
            startTimeEpochMs,
            endTimeEpochMs,
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"),
            Collections.emptyList());
    return chunkManager.query(searchQuery, Duration.ofMillis(3000)).hits.size();
  }

  @Test
  public void testAddAndSearchMessageInMultipleSlices() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new MessageSizeOrCountBasedRolloverStrategy(metricsRegistry, 10 * 1024 * 1024 * 1024L, 10L);

    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService());

    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 15, 1, Instant.now());
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    chunkManager.getActiveChunk().commit();

    await().until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(15);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2);
    testChunkManagerSearch(chunkManager, "Message11", 1, 2, 2);
    testChunkManagerSearch(chunkManager, "Message1 OR Message11", 2, 2, 2);

    checkMetadata(3, 2, 1, 2, 1);
  }

  @Test
  public void testAddAndSearchMessageInSpecificChunks() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new MessageSizeOrCountBasedRolloverStrategy(metricsRegistry, 10 * 1024 * 1024 * 1024L, 10L);

    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService());

    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 15, 1, Instant.now());
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    chunkManager.getActiveChunk().commit();

    await().until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(15);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2);
    testChunkManagerSearch(chunkManager, "Message11", 1, 2, 2);
    testChunkManagerSearch(chunkManager, "Message1 OR Message11", 2, 2, 2);

    checkMetadata(3, 2, 1, 2, 1);

    // Test searching specific chunks
    // Contains messages 11-15
    String activeChunkId = chunkManager.getActiveChunk().info().chunkId;
    assertThat(activeChunkId).isNotEmpty();
    // Contains messages 1-10
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    String firstChunkId =
        chunkManager.chunkList.stream()
            .filter(c -> !c.id().equals(activeChunkId))
            .findFirst()
            .get()
            .id();
    assertThat(firstChunkId).isNotEmpty();

    // Test message in a specific chunk
    testChunkManagerSearch(chunkManager, List.of(firstChunkId), "Message1", 1, 1, 1);
    testChunkManagerSearch(chunkManager, List.of(activeChunkId), "Message11", 1, 1, 1);
    testChunkManagerSearch(chunkManager, List.of(activeChunkId), "Message1 OR Message11", 1, 1, 1);
    testChunkManagerSearch(chunkManager, List.of(firstChunkId), "Message1 OR Message11", 1, 1, 1);
    testChunkManagerSearch(
        chunkManager, List.of(firstChunkId, activeChunkId), "Message1 OR Message11", 2, 2, 2);
    // Search returns empty results
    testChunkManagerSearch(chunkManager, List.of(activeChunkId), "Message1", 0, 1, 1);
    testChunkManagerSearch(chunkManager, List.of(firstChunkId), "Message11", 0, 1, 1);
    testChunkManagerSearch(
        chunkManager, List.of(firstChunkId, activeChunkId), "Message111", 0, 2, 2);
    // test invalid chunk id
    testChunkManagerSearch(chunkManager, List.of("invalidChunkId"), "Message1", 0, 0, 0);
    testChunkManagerSearch(
        chunkManager, List.of("invalidChunkId", firstChunkId), "Message1", 1, 1, 1);
    testChunkManagerSearch(
        chunkManager, List.of("invalidChunkId", activeChunkId), "Message1", 0, 1, 1);
    testChunkManagerSearch(
        chunkManager, List.of("invalidChunkId", firstChunkId, activeChunkId), "Message1", 1, 2, 2);
    testChunkManagerSearch(
        chunkManager, List.of("invalidChunkId", firstChunkId, activeChunkId), "Message11", 1, 2, 2);
    testChunkManagerSearch(
        chunkManager,
        List.of("invalidChunkId", firstChunkId, activeChunkId),
        "Message1 OR Message11",
        2,
        2,
        2);
    testChunkManagerSearch(
        chunkManager,
        List.of("invalidChunkId", firstChunkId, activeChunkId),
        "Message111 OR Message11",
        1,
        2,
        2);
    testChunkManagerSearch(
        chunkManager,
        List.of("invalidChunkId", firstChunkId, activeChunkId),
        "Message111",
        0,
        2,
        2);
  }

  // TODO: Add a unit test where the chunk manager uses a different field conflict policy like
  // RAISE_ERROR.

  @Test
  public void testAddMessageWithPropertyTypeConflicts() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new MessageSizeOrCountBasedRolloverStrategy(metricsRegistry, 10 * 1024 * 1024 * 1024L, 10L);

    ListeningExecutorService rollOverExecutor = IndexingChunkManager.makeDefaultRollOverExecutor();
    initChunkManager(chunkRollOverStrategy, S3_TEST_BUCKET, rollOverExecutor);

    // Add a message
    int offset = 1;
    Trace.Span msg1 = SpanUtil.makeSpan(1);
    chunkManager.addMessage(msg1, msg1.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
    offset++;

    // Add an invalid message
    Trace.Span invalidSpan =
        Trace.Span.newBuilder()
            .setId(ByteString.copyFromUtf8("Message100"))
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setVInt32(20000)
                    .setKey(LogMessage.ReservedField.HOSTNAME.fieldName)
                    .setFieldType(Schema.SchemaFieldType.INTEGER)
                    .build())
            .build();
    chunkManager.addMessage(
        invalidSpan, invalidSpan.getSerializedSize(), TEST_KAFKA_PARTITION_ID, offset);
    offset++;

    // Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();

    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    testChunkManagerSearch(chunkManager, "Message1", 1, 1, 1);
    testChunkManagerSearch(chunkManager, "Message100", 1, 1, 1);

    // Check metadata.
    checkMetadata(1, 1, 0, 1, 1);
  }

  private void checkMetadata(
      int expectedSnapshotSize,
      int expectedLiveSnapshotSize,
      int expectedNonLiveSnapshotSize,
      int expectedSearchNodeSize,
      int expectedInfinitySnapshotsCount) {
    List<SnapshotMetadata> snapshots =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshots.size()).isEqualTo(expectedSnapshotSize);
    List<SnapshotMetadata> liveSnapshots = fetchLiveSnapshot(snapshots);
    assertThat(liveSnapshots.size()).isEqualTo(expectedLiveSnapshotSize);
    assertThat(fetchNonLiveSnapshot(snapshots).size()).isEqualTo(expectedNonLiveSnapshotSize);
    List<SearchMetadata> searchNodes = KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore);
    assertThat(searchNodes.size()).isEqualTo(expectedSearchNodeSize);
    assertThat(liveSnapshots.stream().map(s -> s.snapshotId).collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(
            searchNodes.stream().map(s -> s.snapshotName).collect(Collectors.toList()));
    assertThat(snapshots.stream().filter(s -> s.endTimeEpochMs == MAX_FUTURE_TIME).count())
        .isEqualTo(expectedInfinitySnapshotsCount);
  }

  @Test
  public void testMessagesAddedToActiveChunks() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(metricsRegistry, 10 * 1024 * 1024 * 1024L, 2L);

    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService());

    // Add a message
    List<Trace.Span> msgs = SpanUtil.makeSpansWithTimeDifference(1, 4, 1000, Instant.now());
    Trace.Span msg1 = msgs.get(0);
    Trace.Span msg2 = msgs.get(1);
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

    // Wait for roll over to complete.
    await().until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    testChunkManagerSearch(chunkManager, "Message2", 1, 1, 1);
    checkMetadata(2, 1, 1, 1, 0);

    Trace.Span msg3 = msgs.get(2);
    Trace.Span msg4 = msgs.get(3);
    chunkManager.addMessage(msg3, msg3.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
    offset++;
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(1);
    // Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();
    testChunkManagerSearch(chunkManager, "Message3", 1, 2, 2);
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2);

    checkMetadata(3, 2, 1, 2, 1);
    // Inserting in an older chunk throws an exception. So, additions go to active chunks only.
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> chunk1.addMessage(msg4, TEST_KAFKA_PARTITION_ID, 1));
  }

  @Test
  public void testMultiThreadedChunkRollover() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(metricsRegistry, 10 * 1024 * 1024 * 1024L, 10L);

    ListeningExecutorService rollOverExecutor = IndexingChunkManager.makeDefaultRollOverExecutor();
    initChunkManager(chunkRollOverStrategy, S3_TEST_BUCKET, rollOverExecutor);

    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 25, 1, Instant.now());
    // Add 11 messages to initiate first roll over.
    int offset = 1;
    for (Trace.Span m : messages.subList(0, 11)) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    // Main chunk is already committed. Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();
    // Wait for roll over.
    await().until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 1);

    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(11);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2);
    testChunkManagerSearch(chunkManager, "Message11", 1, 2, 2);
    testChunkManagerSearch(chunkManager, "Message21", 0, 2, 2);
    checkMetadata(3, 2, 1, 2, 1);
  }

  // Adding messages to an already rolled over chunk fails.
  @Test
  public void testAddMessagesToChunkWithRollover() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(metricsRegistry, 10 * 1024 * 1024 * 1024L, 10L);

    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService());

    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 25, 1, Instant.now());
    // Add 11 messages to initiate first roll over.
    int offset = 1;
    for (Trace.Span m : messages.subList(0, 11)) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    // Main chunk is already committed. Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();
    await().until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    checkMetadata(3, 2, 1, 2, 1);
    ChunkInfo secondChunk = chunkManager.getActiveChunk().info();
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(11);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    checkMetadata(3, 2, 1, 2, 1);
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2);
    testChunkManagerSearch(chunkManager, "Message11", 1, 2, 2);
    testChunkManagerSearch(chunkManager, "Message21", 0, 2, 2);

    // Add remaining messages to create a second chunk.
    for (Trace.Span m : messages.subList(11, 25)) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    chunkManager.getActiveChunk().commit();
    await().until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 2);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(3);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(25);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    testChunkManagerSearch(chunkManager, "Message1", 1, 3, 3);
    testChunkManagerSearch(chunkManager, "Message11", 1, 3, 3);
    testChunkManagerSearch(chunkManager, "Message21", 1, 3, 3);
    checkMetadata(5, 3, 2, 3, 1);

    assertThat(chunkManager.getActiveChunk().info().getChunkSnapshotTimeEpochMs()).isZero();
    chunkManager.rollOverActiveChunk();
    for (Chunk<LogMessage> c : chunkManager.getChunkList()) {
      assertThat(c.info().getChunkSnapshotTimeEpochMs()).isGreaterThan(0);
      assertThat(c.info().getDataEndTimeEpochMs()).isGreaterThan(0);
    }

    await().until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 3);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);

    // Search all messages.
    for (int i = 1; i <= 25; i++) {
      testChunkManagerSearch(chunkManager, "Message" + i, 1, 3, 3);
    }
    // No search results for this query.
    testChunkManagerSearch(chunkManager, "Message261", 0, 3, 3);

    checkMetadata(6, 3, 3, 3, 0);
    testOneFailedChunk(secondChunk);
  }

  private void testOneFailedChunk(ChunkInfo secondChunk) {
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    ReadWriteChunk<LogMessage> chunk =
        (ReadWriteChunk<LogMessage>)
            chunkManager.getChunkList().stream()
                .filter(chunkIterator -> Objects.equals(chunkIterator.id(), secondChunk.chunkId))
                .findFirst()
                .get();

    testChunkManagerSearch(chunkManager, "Message18", 1, 3, 3);
    // chunk 2 which has docs 12-21 is corrupted
    // an alternate approach I tried was the statement below
    // chunk.getLogSearcher().close();
    // this worked but was kinda flaky since it messes with shutdown and refresh intervals
    chunk.setLogSearcher(new AlreadyClosedLogIndexSearcherImpl());

    testChunkManagerSearch(chunkManager, "Message18", 0, 3, 2);
    testChunkManagerSearch(chunkManager, "Message1", 1, 3, 2);
    testChunkManagerSearch(chunkManager, "Message25", 1, 3, 2);
  }

  @Test
  public void testAllChunkFailures() throws Exception {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(metricsRegistry, 10 * 1024 * 1024 * 1024L, 10L);

    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService());

    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 25, 1, Instant.now());
    // Add 11 messages to initiate first roll over.
    int offset = 1;
    for (Trace.Span m : messages.subList(0, 11)) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    // Main chunk is already committed. Commit the new chunk so we can search it.
    chunkManager.getActiveChunk().commit();

    await().until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(11);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    checkMetadata(3, 2, 1, 2, 1);
    // TODO: Test commit and refresh count
    testChunkManagerSearch(chunkManager, "Message1", 1, 2, 2);
    testChunkManagerSearch(chunkManager, "Message11", 1, 2, 2);
    testChunkManagerSearch(chunkManager, "Message21", 0, 2, 2);

    for (Trace.Span m : messages.subList(11, 25)) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    chunkManager.getActiveChunk().commit();
    await().until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 2);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(3);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(25);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    checkMetadata(5, 3, 2, 3, 1);
    testChunkManagerSearch(chunkManager, "Message1", 1, 3, 3);
    testChunkManagerSearch(chunkManager, "Message11", 1, 3, 3);
    testChunkManagerSearch(chunkManager, "Message21", 1, 3, 3);

    // Close the log searcher on chunks.
    chunkManager
        .getChunkList()
        .forEach(
            chunk ->
                ((ReadWriteChunk<LogMessage>) chunk)
                    .setLogSearcher(new AlreadyClosedLogIndexSearcherImpl()));

    testChunkManagerSearch(chunkManager, "Message1", 0, 3, 0);
    testChunkManagerSearch(chunkManager, "Message11", 0, 3, 0);
    testChunkManagerSearch(chunkManager, "Message21", 0, 3, 0);

    // Query interface throws search exceptions.
    chunkManager
        .getChunkList()
        .forEach(
            chunk ->
                ((ReadWriteChunk<LogMessage>) chunk)
                    .setLogSearcher(new IllegalArgumentLogIndexSearcherImpl()));

    Throwable throwable =
        catchThrowable(() -> searchAndGetHitCount(chunkManager, "Message1", 0, MAX_TIME));
    assertThat(Throwables.getRootCause(throwable)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCommitInvalidChunk() throws Exception {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

    final List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 10, 1000, startTime);
    messages.addAll(
        SpanUtil.makeSpansWithTimeDifference(11, 20, 1000, startTime.plus(2, ChronoUnit.HOURS)));
    messages.addAll(
        SpanUtil.makeSpansWithTimeDifference(21, 30, 1000, startTime.plus(4, ChronoUnit.HOURS)));

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(metricsRegistry, 10 * 1024 * 1024 * 1024L, 10L);
    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService());

    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    await().until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 3);
    assertThat(chunkManager.getChunkList().size()).isEqualTo(3);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(30);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    checkMetadata(6, 3, 3, 3, 0);

    // No new chunk left to commit.
    assertThat(chunkManager.getActiveChunk()).isNull();
  }

  // TODO: Ensure search at ms slices. Currently at sec resolution?

  @Test
  public void testMultiChunkSearch() throws Exception {
    final Instant startTime = Instant.now();

    final List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 10, 1000, startTime);
    messages.addAll(
        SpanUtil.makeSpansWithTimeDifference(11, 20, 1000, startTime.plus(2, ChronoUnit.MINUTES)));
    messages.addAll(
        SpanUtil.makeSpansWithTimeDifference(21, 30, 1000, startTime.plus(4, ChronoUnit.MINUTES)));
    messages.addAll(
        SpanUtil.makeSpansWithTimeDifference(31, 35, 1000, startTime.plus(6, ChronoUnit.MINUTES)));

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(metricsRegistry, 10 * 1024 * 1024 * 1024L, 10L);
    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService());

    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    // Main chunk is already committed. Commit the new chunk so we can search it.
    await().until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 3);
    chunkManager.getActiveChunk().commit();
    assertThat(chunkManager.getChunkList().size()).isEqualTo(4);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(35);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    checkMetadata(7, 4, 3, 4, 1);
    // TODO: Test commit and refresh count

    final long messagesStartTimeMs =
        TimeUnit.MILLISECONDS.convert(messages.get(0).getTimestamp(), TimeUnit.MICROSECONDS);

    // Search all messages
    for (int i = 1; i <= 35; i++) {
      assertThat(searchAndGetHitCount(chunkManager, "Message" + i, 0, MAX_TIME)).isEqualTo(1);
    }

    // 0 to MAX_TIME
    assertThat(searchAndGetHitCount(chunkManager, "Message1 OR Message25", 0, MAX_TIME))
        .isEqualTo(2);

    assertThat(searchAndGetHitCount(chunkManager, "Message2", messagesStartTimeMs, MAX_TIME))
        .isEqualTo(1);

    assertThat(searchAndGetHitCount(chunkManager, "Message1", messagesStartTimeMs, MAX_TIME))
        .isEqualTo(1);

    // Message1 & chunk 1
    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + 10000;
    // TODO: test chunk metadata.
    assertThat(searchAndGetHitCount(chunkManager, "Message1", chunk1StartTimeMs, chunk1EndTimeMs))
        .isEqualTo(1);

    assertThat(searchAndGetHitCount(chunkManager, "Message1", chunk1StartTimeMs, chunk1EndTimeMs))
        .isEqualTo(1);

    assertThat(
            searchAndGetHitCount(
                chunkManager, "Message1", chunk1StartTimeMs + 2000, chunk1EndTimeMs))
        .isEqualTo(0);

    assertThat(
            searchAndGetHitCount(
                chunkManager, "Message10", chunk1StartTimeMs + 2000, chunk1EndTimeMs))
        .isEqualTo(1);

    // Message 11 & chunk 2
    assertThat(
            searchAndGetHitCount(
                chunkManager, "Message11", messagesStartTimeMs, messagesStartTimeMs + 10000))
        .isEqualTo(0);

    final long chunk2StartTimeMs = chunk1StartTimeMs + Duration.ofMinutes(2).toMillis();
    final long chunk2EndTimeMs = chunk2StartTimeMs + 10000;

    assertThat(searchAndGetHitCount(chunkManager, "Message11", chunk2StartTimeMs, chunk2EndTimeMs))
        .isEqualTo(1);

    assertThat(searchAndGetHitCount(chunkManager, "Message20", chunk2StartTimeMs, chunk2EndTimeMs))
        .isEqualTo(1);

    assertThat(searchAndGetHitCount(chunkManager, "Message21", chunk2StartTimeMs, chunk2EndTimeMs))
        .isEqualTo(0);

    // Chunk 3
    final long chunk3StartTimeMs = chunk1StartTimeMs + Duration.ofMinutes(4).toMillis();
    final long chunk3EndTimeMs = chunk3StartTimeMs + 10000;

    assertThat(searchAndGetHitCount(chunkManager, "Message21", chunk3StartTimeMs, chunk3EndTimeMs))
        .isEqualTo(1);

    assertThat(searchAndGetHitCount(chunkManager, "Message30", chunk3StartTimeMs, chunk3EndTimeMs))
        .isEqualTo(1);

    assertThat(searchAndGetHitCount(chunkManager, "Message31", chunk3StartTimeMs, chunk3EndTimeMs))
        .isEqualTo(0);

    // Chunk 4
    final long chunk4StartTimeMs = chunk1StartTimeMs + Duration.ofMinutes(6).toMillis();
    final long chunk4EndTimeMs = chunk4StartTimeMs + 10000;

    assertThat(searchAndGetHitCount(chunkManager, "Message31", chunk4StartTimeMs, chunk4EndTimeMs))
        .isEqualTo(1);

    assertThat(searchAndGetHitCount(chunkManager, "Message35", chunk4StartTimeMs, chunk4EndTimeMs))
        .isEqualTo(1);

    assertThat(searchAndGetHitCount(chunkManager, "Message36", chunk4StartTimeMs, chunk4EndTimeMs))
        .isEqualTo(0);

    // TODO: Test the entire search response in all queries and not just hits.
  }

  @Test
  public void testChunkRollOverInProgressExceptionIsThrown() throws Exception {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

    final List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 20, 1000, startTime);

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(metricsRegistry, 10 * 1024 * 1024 * 1024L, 10L);
    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, IndexingChunkManager.makeDefaultRollOverExecutor());

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();

    assertThat(fetchLiveSnapshot(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)))
        .isEmpty();

    assertThat(fetchNonLiveSnapshot(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)))
        .isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore)).isEmpty();

    // Adding a messages very quickly when running a rollover in background would result in an
    // exception.
    assertThatThrownBy(
            () -> {
              int offset = 1;
              for (Trace.Span m : messages) {
                chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
                offset++;
              }
            })
        .isInstanceOf(ChunkRollOverException.class);

    List<SnapshotMetadata> snapshots =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshots.size()).isEqualTo(2);
    List<SnapshotMetadata> liveSnapshots = fetchLiveSnapshot(snapshots);
    assertThat(liveSnapshots.size()).isGreaterThanOrEqualTo(2);
    assertThat(fetchNonLiveSnapshot(snapshots).size()).isEqualTo(0);
    List<SearchMetadata> searchNodes = KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore);
    assertThat(searchNodes.size()).isEqualTo(2);
    assertThat(liveSnapshots.stream().map(s -> s.snapshotId).collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(
            searchNodes.stream().map(s -> s.snapshotName).collect(Collectors.toList()));
    assertThat(snapshots.stream().filter(s -> s.endTimeEpochMs == MAX_FUTURE_TIME).count())
        .isEqualTo(2);
  }

  @Test
  public void testSuccessfulRollOverFinishesOnClose() throws Exception {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    final List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 10, 1000, startTime);

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(metricsRegistry, 10 * 1024 * 1024 * 1024L, 10L);
    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, IndexingChunkManager.makeDefaultRollOverExecutor());

    // Adding a message and close the chunkManager right away should still finish the failed
    // rollover.
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    ListenableFuture<?> rollOverFuture = chunkManager.getRolloverFuture();

    await().until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    checkMetadata(2, 1, 1, 1, 0);
    chunkManager.stopAsync();
    chunkManager.awaitTerminated(DEFAULT_START_STOP_DURATION);
    chunkManager = null;

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(rollOverFuture.isDone()).isTrue();

    // The stores are closed so temporarily re-create them so we can query the data in ZK.
    SearchMetadataStore searchMetadataStore = new SearchMetadataStore(curatorFramework, false);
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(curatorFramework);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore)).isEmpty();
    List<SnapshotMetadata> snapshots =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshots.size()).isEqualTo(1);
    assertThat(fetchNonLiveSnapshot(snapshots).size()).isEqualTo(1);
    assertThat(fetchLiveSnapshot(snapshots)).isEmpty();
    assertThat(snapshots.get(0).maxOffset).isEqualTo(offset - 1);
    assertThat(snapshots.get(0).endTimeEpochMs).isLessThan(MAX_FUTURE_TIME);
    assertThat(snapshots.get(0).snapshotId).doesNotContain(SnapshotMetadata.LIVE_SNAPSHOT_PATH);
    searchMetadataStore.close();
    snapshotMetadataStore.close();
  }

  @Test
  @Disabled // flaky test
  public void testFailedRollOverFinishesOnClose() throws Exception {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 10, 1, startTime);

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(metricsRegistry, 10 * 1024 * 1024 * 1024L, 10L);
    initChunkManager(
        chunkRollOverStrategy,
        S3_TEST_BUCKET + "Fail",
        IndexingChunkManager.makeDefaultRollOverExecutor());

    // Adding a message and close the chunkManager right away should still finish the failed
    // rollover.
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    await().until(() -> getCount(ROLLOVERS_FAILED, metricsRegistry) == 1);
    checkMetadata(1, 1, 0, 1, 1);
    ListenableFuture<?> rollOverFuture = chunkManager.getRolloverFuture();
    chunkManager.stopAsync();
    chunkManager.awaitTerminated(15, TimeUnit.SECONDS);
    chunkManager = null;

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(0);
    assertThat(rollOverFuture.isDone()).isTrue();

    // The stores are closed so temporarily re-create them so we can query the data in ZK.
    // All ephemeral data is ZK is deleted and no data or metadata is persisted.
    SearchMetadataStore searchMetadataStore = new SearchMetadataStore(curatorFramework, false);
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(curatorFramework);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    searchMetadataStore.close();
    snapshotMetadataStore.close();
    // Data is lost and the indexer, we use recovery indexer to re-index this data.
  }

  @Test
  public void testRollOverFailure()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 10, 1, startTime);

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(metricsRegistry, 10 * 1024 * 1024 * 1024L, 10L);
    initChunkManager(
        chunkRollOverStrategy,
        S3_TEST_BUCKET + "Fail",
        IndexingChunkManager.makeDefaultRollOverExecutor());

    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> getCount(ROLLOVERS_FAILED, metricsRegistry) == 1);
    assertThat(getTimerCount(ROLLOVER_TIMER, metricsRegistry)).isEqualTo(1);
    checkMetadata(1, 1, 0, 1, 1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(0);

    // Adding a message after a rollover fails throws an exception.
    assertThatThrownBy(
            () -> {
              int newOffset = 1;
              List<Trace.Span> newMessage =
                  SpanUtil.makeSpansWithTimeDifference(11, 12, 1000, startTime);
              for (Trace.Span m : newMessage) {
                chunkManager.addMessage(
                    m, m.toString().length(), TEST_KAFKA_PARTITION_ID, newOffset);
                newOffset++;
              }
            })
        .isInstanceOf(ChunkRollOverException.class);
    checkMetadata(1, 1, 0, 1, 1);
  }

  @Test
  public void testRollOverFailureWithDirectExecutor()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 10, 1000, Instant.now());

    final ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(metricsRegistry, 10 * 1024 * 1024 * 1024L, 10L);
    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET + "Fail", MoreExecutors.newDirectExecutorService());

    // Adding a messages very quickly when running a rollover in background would result in an
    // exception.
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }

    await().until(() -> getCount(ROLLOVERS_FAILED, metricsRegistry) == 1);
    checkMetadata(1, 1, 0, 1, 1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(0);

    // Adding a message after a rollover fails throws an exception.
    assertThatThrownBy(
            () -> {
              int newOffset = 1000;
              List<Trace.Span> newMessage =
                  SpanUtil.makeSpansWithTimeDifference(11, 12, 1000, Instant.now());
              for (Trace.Span m : newMessage) {
                chunkManager.addMessage(
                    m, m.toString().length(), TEST_KAFKA_PARTITION_ID, newOffset);
                newOffset++;
              }
            })
        .isInstanceOf(ChunkRollOverException.class);
    checkMetadata(1, 1, 0, 1, 1);
  }

  @Test
  public void testNewFieldAddedToSchema() throws IOException, TimeoutException {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(
            metricsRegistry, 10 * 1024 * 1024 * 1024L, 100L);
    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, MoreExecutors.newDirectExecutorService());

    List<Trace.Span> messages1 = SpanUtil.makeSpansWithTimeDifference(1, 10, 1, Instant.now());
    Map<String, FieldType> schemaBefore = chunkManager.getSchema();
    assertThat(schemaBefore.size()).isEqualTo(0);

    int offset = 1;
    for (Trace.Span m : messages1) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    chunkManager.getActiveChunk().commit();
    chunkManager.rollOverActiveChunk();
    await().until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 1);

    // add a new message with a novel field and value
    Trace.KeyValue schemaTestTag =
        Trace.KeyValue.newBuilder()
            .setKey("schemaTest")
            .setFieldType(Schema.SchemaFieldType.BOOLEAN)
            .setVBool(true)
            .build();
    Trace.Span logMessage =
        SpanUtil.makeSpan(11, "Message11", Instant.now(), List.of(schemaTestTag));

    chunkManager.addMessage(
        logMessage, logMessage.toString().length(), TEST_KAFKA_PARTITION_ID, offset++);
    chunkManager.rollOverActiveChunk();
    await().until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 2);

    // ensure that we have the new field, and as well as at least one other field
    Map<String, FieldType> schema = chunkManager.getSchema();
    assertThat(schema.size()).isGreaterThan(1);
    assertThat(schema.get("schemaTest")).isEqualTo(FieldType.BOOLEAN);
  }

  @Test
  public void testMultipleByteRollOversSuccessfully()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 6, 1000, startTime);

    final long msgsPerChunk = 3L;
    final long maxBytesPerChunk = 100L;
    final ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(
            metricsRegistry, maxBytesPerChunk, msgsPerChunk);
    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, IndexingChunkManager.makeDefaultRollOverExecutor());

    List<Trace.Span> messages1 = messages.subList(0, 3);
    List<Trace.Span> messages2 = messages.subList(3, 6);

    // Add first set of messages, wait for roll over, then add next set of messages.
    insertMessages(chunkManager, messages1, msgsPerChunk);

    await()
        .atMost(15, TimeUnit.SECONDS)
        .until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    checkMetadata(2, 1, 1, 1, 0);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_BYTES_DIR, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);

    insertMessages(chunkManager, messages2, msgsPerChunk);

    await()
        .atMost(15, TimeUnit.SECONDS)
        .until(() -> getCount(ROLLOVERS_COMPLETED, metricsRegistry) == 2);
    checkMetadata(4, 2, 2, 2, 0);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(6);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();

    // Main chunk is already committed. Commit the new chunk so we can search it.
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(6);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_BYTES_DIR, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
  }

  @Test
  @Disabled // flaky
  public void testMultipleCountRollOversSuccessfully()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 20, 1000, startTime);

    final long msgsPerChunk = 10L;
    final ChunkRollOverStrategy chunkRollOverStrategy =
        new DiskOrMessageCountBasedRolloverStrategy(
            metricsRegistry, 10 * 1024 * 1024 * 1024L, msgsPerChunk);
    initChunkManager(
        chunkRollOverStrategy, S3_TEST_BUCKET, IndexingChunkManager.makeDefaultRollOverExecutor());

    List<Trace.Span> messages1 = messages.subList(0, 10);
    List<Trace.Span> messages2 = messages.subList(10, 20);

    // Add first set of messages, wait for roll over, then add next set of messages.
    insertMessages(chunkManager, messages1, msgsPerChunk);

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 1);
    checkMetadata(2, 1, 1, 1, 0);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_BYTES_DIR, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);

    insertMessages(chunkManager, messages2, msgsPerChunk);

    await().until(() -> getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry) == 2);
    checkMetadata(4, 2, 2, 2, 0);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(20);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManager.getRolloverFuture().get(5, TimeUnit.SECONDS);
    assertThat(chunkManager.getRolloverFuture().isDone()).isTrue();

    // Main chunk is already committed. Commit the new chunk so we can search it.
    assertThat(chunkManager.getChunkList().size()).isEqualTo(2);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(20);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_MESSAGES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_BYTES_INDEXED, metricsRegistry)).isEqualTo(0);
    assertThat(getValue(LIVE_BYTES_DIR, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(2);
    assertThat(getTimerCount(ROLLOVER_TIMER, metricsRegistry)).isEqualTo(2);
  }

  private void insertMessages(
      IndexingChunkManager<LogMessage> chunkManager, List<Trace.Span> messages, long msgsPerChunk)
      throws IOException {
    int actualMessagesGauge = 0;
    int actualBytesGauge = 0;
    int offset = 1;
    for (Trace.Span m : messages) {
      final int msgSize = m.toString().length();
      chunkManager.addMessage(m, msgSize, TEST_KAFKA_PARTITION_ID, offset);
      offset++;
      actualMessagesGauge++;
      actualBytesGauge += msgSize;
      if (actualMessagesGauge < msgsPerChunk) {
        final int finalActualMessagesGauge = actualMessagesGauge;
        await()
            .until(
                () -> getValue(LIVE_MESSAGES_INDEXED, metricsRegistry),
                (value) -> value == finalActualMessagesGauge);
        final int finalActualBytesGauge = actualBytesGauge;
        await()
            .until(
                () -> getValue(LIVE_BYTES_INDEXED, metricsRegistry),
                (value) -> value == finalActualBytesGauge);
      } else { // Gauge is reset on roll over
        await()
            .until(() -> getValue(LIVE_MESSAGES_INDEXED, metricsRegistry), (value) -> value == 0);
        await().until(() -> getValue(LIVE_BYTES_INDEXED, metricsRegistry), (value) -> value == 0);
      }
    }
  }
}
