package com.slack.kaldb.server;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.chunk.ChunkManager;
import com.slack.kaldb.chunk.RollOverChunkTask;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.util.JsonUtil;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.*;

public class KaldbLocalSearcherTest {
  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private ChunkManagerUtil<LogMessage> chunkManagerUtil;
  private KaldbLocalSearcher<LogMessage> kaldbLocalSearcher;
  private SimpleMeterRegistry metricsRegistry;

  @Before
  public void setUp() throws InvalidProtocolBufferException {
    KaldbConfigUtil.initEmptyIndexerConfig();
    metricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        new ChunkManagerUtil<>(S3_MOCK_RULE, metricsRegistry, 10 * 1024 * 1024 * 1024L, 100);
    kaldbLocalSearcher = new KaldbLocalSearcher<>(chunkManagerUtil.chunkManager);
  }

  @After
  public void tearDown() throws IOException {
    if (chunkManagerUtil != null) {
      chunkManagerUtil.close();
    }
  }

  @Test
  public void testKalDbSearch() throws IOException {
    ChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
    // No need to commit the active chunk since the last chunk is already closed.
    assertThat(chunkManager.getChunkMap().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(100);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (100 * 1000);

    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();

    KaldbSearch.SearchResult response =
        kaldbLocalSearcher
            .doSearch(
                searchRequestBuilder
                    .setIndexName(MessageUtil.TEST_INDEX_NAME)
                    .setQueryString("Message100")
                    .setStartTimeEpochMs(chunk1StartTimeMs)
                    .setEndTimeEpochMs(chunk1EndTimeMs)
                    .setHowMany(10)
                    .setBucketCount(2)
                    .build())
            .join();

    assertThat(response.getHitsCount()).isEqualTo(1);
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getTotalCount()).isEqualTo(1);
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(0);
    assertThat(response.getTotalSnapshots()).isEqualTo(1);
    assertThat(response.getSnapshotsWithReplicas()).isEqualTo(1);

    // Test hit contents
    assertThat(response.getHits(0)).contains("Message100");
    List<ByteString> hits = response.getHitsList().asByteStringList();
    assertThat(hits.size()).isEqualTo(1);
    LogWireMessage hit = JsonUtil.read(hits.get(0).toStringUtf8(), LogWireMessage.class);
    LogMessage m = LogMessage.fromWireMessage(hit);
    assertThat(m.getType()).isEqualTo(MessageUtil.TEST_MESSAGE_TYPE);
    assertThat(m.getIndex()).isEqualTo(MessageUtil.TEST_INDEX_NAME);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_LONG_PROPERTY)).isEqualTo(100);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_INT_PROPERTY)).isEqualTo(100);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_FLOAT_PROPERTY)).isEqualTo(100.0);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_DOUBLE_PROPERTY)).isEqualTo(100.0);
    assertThat((String) m.source.get("message")).contains("Message100");

    // Test histogram buckets
    assertThat(response.getBucketsList().size()).isEqualTo(2);
    KaldbSearch.HistogramBucket bucket1 = response.getBuckets(0);
    assertThat(bucket1.getCount()).isEqualTo(0);
    assertThat(bucket1.getLow()).isEqualTo(chunk1StartTimeMs);
    assertThat(bucket1.getHigh()).isEqualTo((chunk1StartTimeMs + chunk1EndTimeMs) / 2);
    KaldbSearch.HistogramBucket bucket2 = response.getBuckets(1);
    assertThat(bucket2.getCount()).isEqualTo(1);
    assertThat(bucket2.getHigh()).isEqualTo(chunk1EndTimeMs);

    // TODO: Query multiple chunks.
  }

  @Test
  public void testKalDbSearchNoData() throws IOException {
    ChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
    // No need to commit the active chunk since the last chunk is already closed.

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);

    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();

    KaldbSearch.SearchResult response =
        kaldbLocalSearcher
            .doSearch(
                searchRequestBuilder
                    .setIndexName(MessageUtil.TEST_INDEX_NAME)
                    .setQueryString("blah")
                    .setStartTimeEpochMs(chunk1StartTimeMs)
                    .setEndTimeEpochMs(chunk1EndTimeMs)
                    .setHowMany(10)
                    .setBucketCount(2)
                    .build())
            .join();

    assertThat(response.getHitsCount()).isZero();
    assertThat(response.getTotalCount()).isZero();
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getTotalCount()).isZero();
    assertThat(response.getHitsList().asByteStringList().size()).isZero();
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(0);
    assertThat(response.getTotalSnapshots()).isEqualTo(1);
    assertThat(response.getSnapshotsWithReplicas()).isEqualTo(1);

    // Test histogram buckets
    assertThat(response.getBucketsList().size()).isEqualTo(2);
    KaldbSearch.HistogramBucket bucket1 = response.getBuckets(0);
    assertThat(bucket1.getCount()).isEqualTo(0);
    assertThat(bucket1.getLow()).isEqualTo(chunk1StartTimeMs);
    assertThat(bucket1.getHigh()).isEqualTo((chunk1StartTimeMs + chunk1EndTimeMs) / 2);
    KaldbSearch.HistogramBucket bucket2 = response.getBuckets(1);
    assertThat(bucket2.getCount()).isEqualTo(0);
    assertThat(bucket2.getHigh()).isEqualTo(chunk1EndTimeMs);
  }

  @Test
  public void testKalDbSearchNoHits() throws IOException {
    ChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
    // No need to commit the active chunk since the last chunk is already closed.

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);

    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();

    // TODO: Query multiple chunks.
    KaldbSearch.SearchResult response =
        kaldbLocalSearcher
            .doSearch(
                searchRequestBuilder
                    .setIndexName(MessageUtil.TEST_INDEX_NAME)
                    .setQueryString("Message1")
                    .setStartTimeEpochMs(chunk1StartTimeMs)
                    .setEndTimeEpochMs(chunk1EndTimeMs)
                    .setHowMany(0)
                    .setBucketCount(2)
                    .build())
            .join();

    // Count is 0, but totalCount is 1, since there is 1 hit, but none are to be retrieved.
    assertThat(response.getHitsCount()).isEqualTo(0);
    assertThat(response.getTotalCount()).isEqualTo(1);
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(0);
    assertThat(response.getTotalSnapshots()).isEqualTo(1);
    assertThat(response.getSnapshotsWithReplicas()).isEqualTo(1);
    assertThat(response.getHitsList().asByteStringList().size()).isZero();

    // Test histogram buckets
    assertThat(response.getBucketsList().size()).isEqualTo(2);
    KaldbSearch.HistogramBucket bucket1 = response.getBuckets(0);
    assertThat(bucket1.getCount()).isEqualTo(1);
    assertThat(bucket1.getLow()).isEqualTo(chunk1StartTimeMs);
    assertThat(bucket1.getHigh()).isEqualTo((chunk1StartTimeMs + chunk1EndTimeMs) / 2);
    KaldbSearch.HistogramBucket bucket2 = response.getBuckets(1);
    assertThat(bucket2.getCount()).isEqualTo(0);
    assertThat(bucket2.getHigh()).isEqualTo(chunk1EndTimeMs);
  }

  @Test
  public void testKalDbSearchNoHistogram() throws IOException {
    ChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
    // No need to commit the active chunk since the last chunk is already closed.

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);

    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();

    KaldbSearch.SearchResult response =
        kaldbLocalSearcher
            .doSearch(
                searchRequestBuilder
                    .setIndexName(MessageUtil.TEST_INDEX_NAME)
                    .setQueryString("Message1")
                    .setStartTimeEpochMs(chunk1StartTimeMs)
                    .setEndTimeEpochMs(chunk1EndTimeMs)
                    .setHowMany(10)
                    .setBucketCount(0)
                    .build())
            .join();

    assertThat(response.getHitsCount()).isEqualTo(1);
    assertThat(response.getTotalCount()).isEqualTo(1);
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(0);
    assertThat(response.getTotalSnapshots()).isEqualTo(1);
    assertThat(response.getSnapshotsWithReplicas()).isEqualTo(1);

    // Test hit contents
    assertThat(response.getHitsList().asByteStringList().size()).isEqualTo(1);
    assertThat(response.getHits(0)).contains("Message1");
    List<ByteString> hits = response.getHitsList().asByteStringList();
    assertThat(hits.size()).isEqualTo(1);
    LogWireMessage hit = JsonUtil.read(hits.get(0).toStringUtf8(), LogWireMessage.class);
    LogMessage m = LogMessage.fromWireMessage(hit);
    assertThat(m.getType()).isEqualTo(MessageUtil.TEST_MESSAGE_TYPE);
    assertThat(m.getIndex()).isEqualTo(MessageUtil.TEST_INDEX_NAME);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_LONG_PROPERTY)).isEqualTo(1);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_INT_PROPERTY)).isEqualTo(1);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_FLOAT_PROPERTY)).isEqualTo(1.0);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_DOUBLE_PROPERTY)).isEqualTo(1.0);
    assertThat((String) m.source.get("message")).contains("Message1");

    // Test histogram buckets
    assertThat(response.getBucketsList().size()).isEqualTo(0);
  }

  @Test
  public void testKalDbBadArgSearch() throws Throwable {
    ChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
    // No need to commit the active chunk since the last chunk is already closed.

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);

    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();

    CompletableFuture<KaldbSearch.SearchResult> result =
        kaldbLocalSearcher.doSearch(
            searchRequestBuilder
                .setIndexName(MessageUtil.TEST_INDEX_NAME)
                .setQueryString("Message1")
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(0)
                .setBucketCount(0)
                .build());
    Throwable throwable = catchThrowable(result::get);
    assertThat(throwable.getCause()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testKalDbGrpcSearch() throws IOException {
    // Load test data into chunk manager.
    ChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
    // No need to commit the active chunk since the last chunk is already closed.

    // Setup a InProcess Grpc Server so we can query it.
    // Generate a unique in-process server name.
    String serverName = InProcessServerBuilder.generateName();

    // Create a server, add service, start, and register for automatic graceful shutdown.
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new KaldbLocalSearcher<>(chunkManager))
            .build()
            .start());

    // Create a client channel and register for automatic graceful shutdown.
    KaldbServiceGrpc.KaldbServiceBlockingStub blockingKaldbClient =
        KaldbServiceGrpc.newBlockingStub(
            // Create a client channel and register for automatic graceful shutdown.
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));

    // Build a search request
    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);
    KaldbSearch.SearchResult response =
        blockingKaldbClient.search(
            KaldbSearch.SearchRequest.newBuilder()
                .setIndexName(MessageUtil.TEST_INDEX_NAME)
                .setQueryString("Message1")
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(10)
                .setBucketCount(2)
                .build());

    // Validate search response
    assertThat(response.getHitsCount()).isEqualTo(1);
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getTotalCount()).isEqualTo(1);
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(0);
    assertThat(response.getTotalSnapshots()).isEqualTo(1);
    assertThat(response.getSnapshotsWithReplicas()).isEqualTo(1);

    // Test hit contents
    assertThat(response.getHits(0)).contains("Message1");
    List<ByteString> hits = response.getHitsList().asByteStringList();
    assertThat(hits.size()).isEqualTo(1);
    LogWireMessage hit = JsonUtil.read(hits.get(0).toStringUtf8(), LogWireMessage.class);
    LogMessage m = LogMessage.fromWireMessage(hit);
    assertThat(m.getType()).isEqualTo(MessageUtil.TEST_MESSAGE_TYPE);
    assertThat(m.getIndex()).isEqualTo(MessageUtil.TEST_INDEX_NAME);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_LONG_PROPERTY)).isEqualTo(1);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_INT_PROPERTY)).isEqualTo(1);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_FLOAT_PROPERTY)).isEqualTo(1.0);
    assertThat(m.source.get(MessageUtil.TEST_SOURCE_DOUBLE_PROPERTY)).isEqualTo(1.0);
    assertThat((String) m.source.get("message")).contains("Message1");

    // Test histogram buckets
    assertThat(response.getBucketsList().size()).isEqualTo(2);
    KaldbSearch.HistogramBucket bucket1 = response.getBuckets(0);
    assertThat(bucket1.getCount()).isEqualTo(1);
    assertThat(bucket1.getLow()).isEqualTo(chunk1StartTimeMs);
    assertThat(bucket1.getHigh()).isEqualTo((chunk1StartTimeMs + chunk1EndTimeMs) / 2);
    KaldbSearch.HistogramBucket bucket2 = response.getBuckets(1);
    assertThat(bucket2.getCount()).isEqualTo(0);
    assertThat(bucket2.getHigh()).isEqualTo(chunk1EndTimeMs);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testKalDbGrpcSearchThrowsException() throws IOException {
    // Load test data into chunk manager.
    ChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100, 1000, startTime);
    for (LogMessage m : messages) {
      chunkManager.addMessage(m, m.toString().length(), 100);
    }
    // No need to commit the active chunk since the last chunk is already closed.

    // Setup a InProcess Grpc Server so we can query it.
    // Generate a unique in-process server name.
    String serverName = InProcessServerBuilder.generateName();

    // Create a server, add service, start, and register for automatic graceful shutdown.
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new KaldbLocalSearcher<>(chunkManager))
            .build()
            .start());

    // Create a client channel and register for automatic graceful shutdown.
    KaldbServiceGrpc.KaldbServiceBlockingStub blockingStub =
        KaldbServiceGrpc.newBlockingStub(
            // Create a client channel and register for automatic graceful shutdown.
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));

    // Build a bad search request.
    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);
    KaldbSearch.SearchResult result =
        blockingStub.search(
            KaldbSearch.SearchRequest.newBuilder()
                .setIndexName(MessageUtil.TEST_INDEX_NAME)
                .setQueryString("Message1")
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(0)
                .setBucketCount(0)
                .build());
  }
}
