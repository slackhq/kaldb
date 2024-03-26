package com.slack.kaldb.chunk;

import static com.slack.kaldb.chunk.ReadOnlyChunkImpl.CHUNK_ASSIGNMENT_TIMER;
import static com.slack.kaldb.chunk.ReadOnlyChunkImpl.CHUNK_EVICTION_TIMER;
import static com.slack.kaldb.chunk.ReadWriteChunk.SCHEMA_FILE_NAME;
import static com.slack.kaldb.logstore.BlobFsUtils.copyToS3;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.COMMITS_TIMER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.REFRESHES_TIMER;
import static com.slack.kaldb.proto.metadata.Metadata.IndexType.LOGS_LUCENE9;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.MetricsUtil.getTimerCount;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherExtension.addMessages;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.slack.kaldb.blobfs.LocalBlobFs;
import com.slack.kaldb.blobfs.s3.S3CrtBlobFs;
import com.slack.kaldb.blobfs.s3.S3TestUtils;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.metadata.cache.CacheSlotMetadata;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.core.KaldbMetadataTestUtils;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.schema.ChunkSchema;
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.testlib.MessageUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.lucene.index.IndexCommit;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class ReadOnlyChunkImplTest {
  private static final String TEST_S3_BUCKET = "read-only-chunk-impl-test";

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;
  private S3CrtBlobFs s3CrtBlobFs;

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(TEST_S3_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  @BeforeEach
  public void startup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    S3AsyncClient s3AsyncClient =
        S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
    s3CrtBlobFs = new S3CrtBlobFs(s3AsyncClient);
  }

  @AfterEach
  public void shutdown() throws IOException {
    s3CrtBlobFs.close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldHandleChunkLivecycle() throws Exception {
    KaldbConfigs.KaldbConfig kaldbConfig = makeCacheConfig();
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("shouldHandleChunkLivecycle")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    AsyncCuratorFramework curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(curatorFramework, meterRegistry);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, meterRegistry);
    SearchMetadataStore searchMetadataStore =
        new SearchMetadataStore(curatorFramework, true, meterRegistry);
    CacheSlotMetadataStore cacheSlotMetadataStore =
        new CacheSlotMetadataStore(curatorFramework, meterRegistry);

    String replicaId = "foo";
    String snapshotId = "bar";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(curatorFramework, replicaId, snapshotId);
    initializeZkSnapshot(curatorFramework, snapshotId);
    initializeBlobStorageWithIndex(snapshotId);

    SearchContext searchContext =
        SearchContext.fromConfig(kaldbConfig.getCacheConfig().getServerConfig());
    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new ReadOnlyChunkImpl<>(
            curatorFramework,
            meterRegistry,
            s3CrtBlobFs,
            searchContext,
            kaldbConfig.getS3Config().getS3Bucket(),
            kaldbConfig.getCacheConfig().getDataDirectory(),
            kaldbConfig.getCacheConfig().getReplicaSet(),
            cacheSlotMetadataStore,
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore);

    // wait for chunk to register
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    assignReplicaToChunk(cacheSlotMetadataStore, replicaId, readOnlyChunk);

    // ensure that the chunk was marked LIVE
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 1);
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.LIVE);

    SearchResult<LogMessage> logMessageSearchResult =
        readOnlyChunk.query(
            new SearchQuery(
                MessageUtil.TEST_DATASET_NAME,
                "*:*",
                Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                Instant.now().toEpochMilli(),
                500,
                new DateHistogramAggBuilder(
                    "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"),
                Collections.emptyList()));
    assertThat(logMessageSearchResult.hits.size()).isEqualTo(10);

    await()
        .until(
            () ->
                meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "true").timer().count()
                    == 1);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_EVICTION_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_EVICTION_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(0);

    // ensure we registered a search node for this cache slot
    await().until(() -> searchMetadataStore.listSync().size() == 1);
    assertThat(searchMetadataStore.listSync().get(0).snapshotName).isEqualTo(snapshotId);

    assertThat(searchMetadataStore.listSync().get(0).url).isEqualTo("gproto+http://localhost:8080");
    assertThat(searchMetadataStore.listSync().get(0).name)
        .isEqualTo(SearchMetadata.generateSearchContextSnapshotId(snapshotId, "localhost"));

    // mark the chunk for eviction
    CacheSlotMetadata cacheSlotMetadata =
        cacheSlotMetadataStore.getSync(searchContext.hostname, readOnlyChunk.slotId);
    cacheSlotMetadataStore
        .updateNonFreeCacheSlotState(
            cacheSlotMetadata, Metadata.CacheSlotMetadata.CacheSlotState.EVICT)
        .get(1, TimeUnit.SECONDS);

    // ensure that the evicted chunk was released
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    // ensure the search metadata node was unregistered
    await().until(() -> searchMetadataStore.listSync().size() == 0);

    SearchResult<LogMessage> logMessageEmptySearchResult =
        readOnlyChunk.query(
            new SearchQuery(
                MessageUtil.TEST_DATASET_NAME,
                "*:*",
                Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
                Instant.now().toEpochMilli(),
                500,
                new DateHistogramAggBuilder(
                    "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"),
                Collections.emptyList()));
    assertThat(logMessageEmptySearchResult).isEqualTo(SearchResult.empty());
    assertThat(readOnlyChunk.info()).isNull();

    assertThat(meterRegistry.get(CHUNK_EVICTION_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(1);
    assertThat(meterRegistry.get(CHUNK_EVICTION_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(1);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(0);

    cacheSlotMetadataStore.close();
    searchMetadataStore.close();
    snapshotMetadataStore.close();
    replicaMetadataStore.close();
    curatorFramework.unwrap().close();
  }

  @Test
  public void shouldHandleMissingS3Assets() throws Exception {
    KaldbConfigs.KaldbConfig kaldbConfig = makeCacheConfig();
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("shouldHandleMissingS3Assets")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    AsyncCuratorFramework curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(curatorFramework, meterRegistry);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, meterRegistry);
    SearchMetadataStore searchMetadataStore =
        new SearchMetadataStore(curatorFramework, true, meterRegistry);
    CacheSlotMetadataStore cacheSlotMetadataStore =
        new CacheSlotMetadataStore(curatorFramework, meterRegistry);

    String replicaId = "foo";
    String snapshotId = "bar";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(curatorFramework, replicaId, snapshotId);
    initializeZkSnapshot(curatorFramework, snapshotId);

    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new ReadOnlyChunkImpl<>(
            curatorFramework,
            meterRegistry,
            s3CrtBlobFs,
            SearchContext.fromConfig(kaldbConfig.getCacheConfig().getServerConfig()),
            kaldbConfig.getS3Config().getS3Bucket(),
            kaldbConfig.getCacheConfig().getDataDirectory(),
            kaldbConfig.getCacheConfig().getReplicaSet(),
            cacheSlotMetadataStore,
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore);

    // wait for chunk to register
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    assignReplicaToChunk(cacheSlotMetadataStore, replicaId, readOnlyChunk);

    // assert that the chunk was released back to free
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    // ensure we did not register a search node
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(0);

    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(1);

    cacheSlotMetadataStore.close();
    searchMetadataStore.close();
    snapshotMetadataStore.close();
    replicaMetadataStore.close();
    curatorFramework.unwrap().close();
  }

  @Test
  public void shouldHandleMissingZkData() throws Exception {
    KaldbConfigs.KaldbConfig kaldbConfig = makeCacheConfig();
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("shouldHandleMissingZkData")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    AsyncCuratorFramework curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(curatorFramework, meterRegistry);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, meterRegistry);
    SearchMetadataStore searchMetadataStore =
        new SearchMetadataStore(curatorFramework, true, meterRegistry);
    CacheSlotMetadataStore cacheSlotMetadataStore =
        new CacheSlotMetadataStore(curatorFramework, meterRegistry);

    String replicaId = "foo";
    String snapshotId = "bar";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(curatorFramework, replicaId, snapshotId);
    // we intentionally do not initialize a Snapshot, so the lookup is expected to fail

    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new ReadOnlyChunkImpl<>(
            curatorFramework,
            meterRegistry,
            s3CrtBlobFs,
            SearchContext.fromConfig(kaldbConfig.getCacheConfig().getServerConfig()),
            kaldbConfig.getS3Config().getS3Bucket(),
            kaldbConfig.getCacheConfig().getDataDirectory(),
            kaldbConfig.getCacheConfig().getReplicaSet(),
            cacheSlotMetadataStore,
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore);

    // wait for chunk to register
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    assignReplicaToChunk(cacheSlotMetadataStore, replicaId, readOnlyChunk);

    // assert that the chunk was released back to free
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    // ensure we did not register a search node
    assertThat(searchMetadataStore.listSync().size()).isEqualTo(0);

    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(0);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "false").timer().count())
        .isEqualTo(1);

    cacheSlotMetadataStore.close();
    searchMetadataStore.close();
    snapshotMetadataStore.close();
    replicaMetadataStore.close();
    curatorFramework.unwrap().close();
  }

  @Test
  public void closeShouldCleanupLiveChunkCorrectly() throws Exception {
    KaldbConfigs.KaldbConfig kaldbConfig = makeCacheConfig();
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("shouldHandleChunkLivecycle")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    AsyncCuratorFramework curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(curatorFramework, meterRegistry);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, meterRegistry);
    SearchMetadataStore searchMetadataStore =
        new SearchMetadataStore(curatorFramework, true, meterRegistry);
    CacheSlotMetadataStore cacheSlotMetadataStore =
        new CacheSlotMetadataStore(curatorFramework, meterRegistry);

    String replicaId = "foo";
    String snapshotId = "bar";

    // setup Zk, BlobFs so data can be loaded
    initializeZkReplica(curatorFramework, replicaId, snapshotId);
    initializeZkSnapshot(curatorFramework, snapshotId);
    initializeBlobStorageWithIndex(snapshotId);

    ReadOnlyChunkImpl<LogMessage> readOnlyChunk =
        new ReadOnlyChunkImpl<>(
            curatorFramework,
            meterRegistry,
            s3CrtBlobFs,
            SearchContext.fromConfig(kaldbConfig.getCacheConfig().getServerConfig()),
            kaldbConfig.getS3Config().getS3Bucket(),
            kaldbConfig.getCacheConfig().getDataDirectory(),
            kaldbConfig.getCacheConfig().getReplicaSet(),
            cacheSlotMetadataStore,
            replicaMetadataStore,
            snapshotMetadataStore,
            searchMetadataStore);

    // wait for chunk to register
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.FREE);

    assignReplicaToChunk(cacheSlotMetadataStore, replicaId, readOnlyChunk);

    // ensure that the chunk was marked LIVE
    await()
        .until(
            () ->
                readOnlyChunk.getChunkMetadataState()
                    == Metadata.CacheSlotMetadata.CacheSlotState.LIVE);

    SearchQuery query =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            "*:*",
            Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().toEpochMilli(),
            500,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"),
            Collections.emptyList());
    SearchResult<LogMessage> logMessageSearchResult = readOnlyChunk.query(query);
    assertThat(logMessageSearchResult.hits.size()).isEqualTo(10);
    assertThat(meterRegistry.get(CHUNK_ASSIGNMENT_TIMER).tag("successful", "true").timer().count())
        .isEqualTo(1);

    // ensure we registered a search node for this cache slot
    await().until(() -> searchMetadataStore.listSync().size() == 1);
    assertThat(searchMetadataStore.listSync().get(0).snapshotName).isEqualTo(snapshotId);

    assertThat(searchMetadataStore.listSync().get(0).url).isEqualTo("gproto+http://localhost:8080");
    assertThat(searchMetadataStore.listSync().get(0).name)
        .isEqualTo(SearchMetadata.generateSearchContextSnapshotId(snapshotId, "localhost"));

    // verify we have files on disk
    try (var files = java.nio.file.Files.list(readOnlyChunk.getDataDirectory())) {
      assertThat(files.findFirst().isPresent()).isTrue();
    }

    // attempt to close the readOnlyChunk
    readOnlyChunk.close();

    // verify no results are returned for the exact same query we did above
    SearchResult<LogMessage> logMessageSearchResultEmpty = readOnlyChunk.query(query);
    assertThat(logMessageSearchResultEmpty).isEqualTo(SearchResult.empty());
    assertThat(readOnlyChunk.info()).isNull();

    // verify that the directory has been cleaned up
    try (var files = java.nio.file.Files.list(readOnlyChunk.getDataDirectory())) {
      assertThat(files.findFirst().isPresent()).isFalse();
    }

    cacheSlotMetadataStore.close();
    searchMetadataStore.close();
    snapshotMetadataStore.close();
    replicaMetadataStore.close();
    curatorFramework.unwrap().close();
  }

  @Test
  public void shouldUseOptimizedQueryStartEndTime() {
    // Query is before the chunk data, so do not return a start time
    assertThat(ReadOnlyChunkImpl.determineStartTime(10, 12)).isNull();

    // Query matches chunk start time, do not return a start time
    assertThat(ReadOnlyChunkImpl.determineStartTime(10, 10)).isNull();

    // Query only matches part of the chunk, return the query start time
    assertThat(ReadOnlyChunkImpl.determineStartTime(10, 9)).isEqualTo(10);

    // Query only matches part of the chunk, return the query end time
    assertThat(ReadOnlyChunkImpl.determineEndTime(10, 12)).isEqualTo(10);

    // Query matches chunk end time, do not return an end time
    assertThat(ReadOnlyChunkImpl.determineEndTime(10, 10)).isNull();

    // Query is after the chunk data, so do not return an end time
    assertThat(ReadOnlyChunkImpl.determineEndTime(12, 10)).isNull();
  }

  private void assignReplicaToChunk(
      CacheSlotMetadataStore cacheSlotMetadataStore,
      String replicaId,
      ReadOnlyChunkImpl<LogMessage> readOnlyChunk) {
    // update chunk to assigned
    CacheSlotMetadata updatedCacheSlotMetadata =
        new CacheSlotMetadata(
            readOnlyChunk.slotId,
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicaId,
            Instant.now().toEpochMilli(),
            List.of(LOGS_LUCENE9),
            readOnlyChunk.searchContext.hostname,
            "rep1");
    cacheSlotMetadataStore.updateAsync(updatedCacheSlotMetadata);
  }

  private void initializeZkSnapshot(AsyncCuratorFramework curatorFramework, String snapshotId)
      throws Exception {
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, meterRegistry);
    snapshotMetadataStore.createSync(
        new SnapshotMetadata(
            snapshotId,
            "path",
            Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().toEpochMilli(),
            1,
            "partitionId",
            LOGS_LUCENE9));
  }

  private void initializeZkReplica(
      AsyncCuratorFramework curatorFramework, String replicaId, String snapshotId)
      throws Exception {
    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(curatorFramework, meterRegistry);
    replicaMetadataStore.createSync(
        new ReplicaMetadata(
            replicaId,
            snapshotId,
            "rep1",
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(60).toEpochMilli(),
            false,
            LOGS_LUCENE9));
  }

  private void initializeBlobStorageWithIndex(String snapshotId) throws Exception {
    LuceneIndexStoreImpl logStore =
        LuceneIndexStoreImpl.makeLogStore(
            Files.newTemporaryFolder(),
            Duration.ofSeconds(60),
            Duration.ofSeconds(60),
            true,
            SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_VALUE_AND_DUPLICATE_FIELD,
            meterRegistry);
    addMessages(logStore, 1, 10, true);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, meterRegistry)).isEqualTo(10);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, meterRegistry)).isEqualTo(1);
    assertThat(getTimerCount(COMMITS_TIMER, meterRegistry)).isEqualTo(1);

    Path dirPath = logStore.getDirectory().getDirectory().toAbsolutePath();

    // Create schema file to upload
    ChunkSchema chunkSchema =
        new ChunkSchema(snapshotId, logStore.getSchema(), new ConcurrentHashMap<>());
    File schemaFile = new File(dirPath + "/" + SCHEMA_FILE_NAME);
    ChunkSchema.serializeToFile(chunkSchema, schemaFile);

    // Prepare list of files to upload.
    List<String> filesToUpload = new ArrayList<>();
    filesToUpload.add(schemaFile.getName());
    IndexCommit indexCommit = logStore.getIndexCommit();
    filesToUpload.addAll(indexCommit.getFileNames());

    LocalBlobFs localBlobFs = new LocalBlobFs();

    logStore.close();
    assertThat(localBlobFs.listFiles(dirPath.toUri(), false).length)
        .isGreaterThanOrEqualTo(filesToUpload.size());

    // Copy files to S3.
    copyToS3(dirPath, filesToUpload, TEST_S3_BUCKET, snapshotId, s3CrtBlobFs);
  }

  private KaldbConfigs.KaldbConfig makeCacheConfig() {
    KaldbConfigs.CacheConfig cacheConfig =
        KaldbConfigs.CacheConfig.newBuilder()
            .setSlotsPerInstance(3)
            .setReplicaSet("rep1")
            .setDataDirectory(
                String.format(
                    "/tmp/%s/%s",
                    this.getClass().getSimpleName(), RandomStringUtils.randomAlphabetic(10)))
            .setServerConfig(
                KaldbConfigs.ServerConfig.newBuilder()
                    .setServerAddress("localhost")
                    .setServerPort(8080)
                    .build())
            .build();

    KaldbConfigs.S3Config s3Config =
        KaldbConfigs.S3Config.newBuilder()
            .setS3Bucket(TEST_S3_BUCKET)
            .setS3Region("us-east-1")
            .build();

    return KaldbConfigs.KaldbConfig.newBuilder()
        .setCacheConfig(cacheConfig)
        .setS3Config(s3Config)
        .build();
  }
}
