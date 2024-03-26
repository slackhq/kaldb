package com.slack.kaldb.metadata.cache;

import static com.slack.kaldb.proto.metadata.Metadata.CacheSlotMetadata.CacheSlotState;
import static com.slack.kaldb.proto.metadata.Metadata.IndexType.LOGS_LUCENE9;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.awaitility.Awaitility.await;

import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.core.KaldbMetadataTestUtils;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CacheSlotMetadataStoreTest {
  private static final List<Metadata.IndexType> SUPPORTED_INDEX_TYPES = List.of(LOGS_LUCENE9);

  private TestingServer testingServer;

  private AsyncCuratorFramework curatorFramework;
  private MeterRegistry meterRegistry;
  private CacheSlotMetadataStore store;

  @BeforeEach
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    // NOTE: Sometimes the ZK server fails to start. Handle it more gracefully, if tests are
    // flaky.
    testingServer = new TestingServer();

    KaldbConfigs.ZookeeperConfig zookeeperConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("Test")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(500)
            .build();
    this.curatorFramework = CuratorBuilder.build(meterRegistry, zookeeperConfig);
    this.store = new CacheSlotMetadataStore(curatorFramework, meterRegistry);
  }

  @AfterEach
  public void tearDown() throws IOException {
    store.close();
    curatorFramework.unwrap().close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void testUpdateNonFreeCacheSlotStateSync() throws Exception {
    final String name = "slot1";
    final String hostname = "hostname";
    final String replicaSet = "rep1";
    Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState = CacheSlotState.ASSIGNED;
    final String replicaId = "3456";
    long updatedTimeEpochMs = Instant.now().toEpochMilli();

    final CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            name,
            cacheSlotState,
            replicaId,
            updatedTimeEpochMs,
            SUPPORTED_INDEX_TYPES,
            hostname,
            replicaSet);
    assertThat(cacheSlotMetadata.name).isEqualTo(name);
    assertThat(cacheSlotMetadata.cacheSlotState).isEqualTo(cacheSlotState);
    assertThat(cacheSlotMetadata.replicaId).isEqualTo(replicaId);
    assertThat(cacheSlotMetadata.updatedTimeEpochMs).isEqualTo(updatedTimeEpochMs);
    assertThat(cacheSlotMetadata.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);
    assertThat(cacheSlotMetadata.hostname).isEqualTo(hostname);

    store.createSync(cacheSlotMetadata);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(store).size()).isEqualTo(1);

    store
        .updateNonFreeCacheSlotState(cacheSlotMetadata, CacheSlotState.LIVE)
        .get(1, TimeUnit.SECONDS);
    await()
        .until(
            () ->
                store.listSync().size() == 1
                    && store.listSync().get(0).cacheSlotState == CacheSlotState.LIVE);
    final CacheSlotMetadata liveNode = store.getSync(hostname, name);
    assertThat(liveNode.name).isEqualTo(name);
    assertThat(liveNode.cacheSlotState).isEqualTo(CacheSlotState.LIVE);
    assertThat(liveNode.replicaId).isEqualTo(replicaId);
    assertThat(liveNode.updatedTimeEpochMs).isGreaterThan(cacheSlotMetadata.updatedTimeEpochMs);
    assertThat(liveNode.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    store
        .updateNonFreeCacheSlotState(cacheSlotMetadata, CacheSlotState.EVICT)
        .get(1, TimeUnit.SECONDS);
    await()
        .until(
            () ->
                store.listSync().size() == 1
                    && store.listSync().get(0).cacheSlotState == CacheSlotState.EVICT);
    final CacheSlotMetadata evictNode = store.getSync(hostname, name);
    assertThat(evictNode.name).isEqualTo(name);
    assertThat(evictNode.cacheSlotState).isEqualTo(CacheSlotState.EVICT);
    assertThat(evictNode.replicaId).isEqualTo(replicaId);
    assertThat(evictNode.updatedTimeEpochMs).isGreaterThan(liveNode.updatedTimeEpochMs);
    assertThat(evictNode.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    store
        .updateNonFreeCacheSlotState(cacheSlotMetadata, CacheSlotState.FREE)
        .get(1, TimeUnit.SECONDS);
    await()
        .until(
            () ->
                store.listSync().size() == 1
                    && store.listSync().get(0).cacheSlotState == CacheSlotState.FREE);
    final CacheSlotMetadata freeNode = store.getSync(hostname, name);
    assertThat(freeNode.name).isEqualTo(name);
    assertThat(freeNode.cacheSlotState).isEqualTo(CacheSlotState.FREE);
    assertThat(freeNode.replicaId).isEmpty();
    assertThat(freeNode.updatedTimeEpochMs).isGreaterThan(evictNode.updatedTimeEpochMs);
    assertThat(freeNode.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    // Only non-free states can be set.
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                store
                    .updateNonFreeCacheSlotState(freeNode, CacheSlotState.ASSIGNED)
                    .get(1, TimeUnit.SECONDS));
  }

  @Test
  public void testNonFreeCacheSlotState() throws Exception {
    final String name = "slot1";
    final String hostname = "hostname";
    final String replicaSet = "rep1";
    Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState = CacheSlotState.ASSIGNED;
    final String replicaId = "3456";
    long updatedTimeEpochMs = Instant.now().toEpochMilli();

    final CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            name,
            cacheSlotState,
            replicaId,
            updatedTimeEpochMs,
            SUPPORTED_INDEX_TYPES,
            hostname,
            replicaSet);
    assertThat(cacheSlotMetadata.name).isEqualTo(name);
    assertThat(cacheSlotMetadata.cacheSlotState).isEqualTo(cacheSlotState);
    assertThat(cacheSlotMetadata.replicaId).isEqualTo(replicaId);
    assertThat(cacheSlotMetadata.updatedTimeEpochMs).isEqualTo(updatedTimeEpochMs);
    assertThat(cacheSlotMetadata.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);
    assertThat(cacheSlotMetadata.hostname).isEqualTo(hostname);

    store.createSync(cacheSlotMetadata);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(store).size()).isEqualTo(1);

    store
        .updateNonFreeCacheSlotState(cacheSlotMetadata, CacheSlotState.LIVE)
        .get(1, TimeUnit.SECONDS);
    await()
        .until(
            () ->
                store.listSync().size() == 1
                    && store.listSync().get(0).cacheSlotState == CacheSlotState.LIVE);
    final CacheSlotMetadata liveNode = store.getSync(hostname, name);
    assertThat(liveNode.name).isEqualTo(name);
    assertThat(liveNode.cacheSlotState).isEqualTo(CacheSlotState.LIVE);
    assertThat(liveNode.replicaId).isEqualTo(replicaId);
    assertThat(liveNode.updatedTimeEpochMs).isGreaterThan(cacheSlotMetadata.updatedTimeEpochMs);
    assertThat(liveNode.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    store.updateNonFreeCacheSlotState(liveNode, CacheSlotState.EVICT).get(1, TimeUnit.SECONDS);
    await()
        .until(
            () ->
                store.listSync().size() == 1
                    && store.listSync().get(0).cacheSlotState == CacheSlotState.EVICT);
    final CacheSlotMetadata evictNode = store.getSync(hostname, name);
    assertThat(evictNode.name).isEqualTo(name);
    assertThat(evictNode.cacheSlotState).isEqualTo(CacheSlotState.EVICT);
    assertThat(evictNode.replicaId).isEqualTo(replicaId);
    assertThat(evictNode.updatedTimeEpochMs).isGreaterThan(liveNode.updatedTimeEpochMs);
    assertThat(evictNode.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    store.updateNonFreeCacheSlotState(evictNode, CacheSlotState.FREE).get(1, TimeUnit.SECONDS);
    await()
        .until(
            () ->
                store.listSync().size() == 1
                    && store.listSync().get(0).cacheSlotState == CacheSlotState.FREE);
    final CacheSlotMetadata freeNode = store.getSync(hostname, name);
    assertThat(freeNode.name).isEqualTo(name);
    assertThat(freeNode.cacheSlotState).isEqualTo(CacheSlotState.FREE);
    assertThat(freeNode.replicaId).isEmpty();
    assertThat(freeNode.updatedTimeEpochMs).isGreaterThan(evictNode.updatedTimeEpochMs);
    assertThat(freeNode.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    // Only non-free states can be set.
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                store
                    .updateNonFreeCacheSlotState(freeNode, CacheSlotState.ASSIGNED)
                    .get(1, TimeUnit.SECONDS));
  }

  @Test
  public void testCacheSlotStateWithReplica() throws Exception {
    String name = "slot1";
    final String hostname = "hostname";
    final String replicaSet = "rep1";
    Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState =
        Metadata.CacheSlotMetadata.CacheSlotState.FREE;
    String emptyReplicaId = "";
    long updatedTimeEpochMs = Instant.now().toEpochMilli();

    final CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            name,
            cacheSlotState,
            emptyReplicaId,
            updatedTimeEpochMs,
            SUPPORTED_INDEX_TYPES,
            hostname,
            replicaSet);
    assertThat(cacheSlotMetadata.name).isEqualTo(name);
    assertThat(cacheSlotMetadata.cacheSlotState).isEqualTo(cacheSlotState);
    assertThat(cacheSlotMetadata.replicaId).isEqualTo(emptyReplicaId);
    assertThat(cacheSlotMetadata.updatedTimeEpochMs).isEqualTo(updatedTimeEpochMs);
    assertThat(cacheSlotMetadata.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);
    assertThat(cacheSlotMetadata.hostname).isEqualTo(hostname);

    store.createSync(cacheSlotMetadata);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(store).size()).isEqualTo(1);

    store
        .updateCacheSlotStateStateWithReplicaId(cacheSlotMetadata, cacheSlotState, "")
        .get(1, TimeUnit.SECONDS);
    await()
        .until(
            () ->
                store.listSync().size() == 1
                    && store.listSync().get(0).cacheSlotState == cacheSlotState);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(store).size()).isEqualTo(1);
    final CacheSlotMetadata freeNode = store.getSync(hostname, name);
    assertThat(freeNode.name).isEqualTo(name);
    assertThat(freeNode.cacheSlotState).isEqualTo(cacheSlotState);
    assertThat(freeNode.replicaId).isEqualTo(emptyReplicaId);
    assertThat(freeNode.updatedTimeEpochMs).isGreaterThanOrEqualTo(updatedTimeEpochMs);
    assertThat(freeNode.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    final String replicaId = "1234";
    store
        .updateCacheSlotStateStateWithReplicaId(
            cacheSlotMetadata, Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED, replicaId)
        .get(1, TimeUnit.SECONDS);
    await()
        .until(
            () ->
                store.listSync().size() == 1
                    && store.listSync().get(0).cacheSlotState == CacheSlotState.ASSIGNED);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(store).size()).isEqualTo(1);
    CacheSlotMetadata assignedNode = store.getSync(hostname, name);
    assertThat(assignedNode.name).isEqualTo(name);
    assertThat(assignedNode.cacheSlotState)
        .isEqualTo(Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED);
    assertThat(assignedNode.replicaId).isEqualTo(replicaId);
    assertThat(assignedNode.updatedTimeEpochMs).isGreaterThan(freeNode.updatedTimeEpochMs);
    assertThat(assignedNode.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);

    store
        .updateCacheSlotStateStateWithReplicaId(
            cacheSlotMetadata, Metadata.CacheSlotMetadata.CacheSlotState.EVICT, replicaId)
        .get(1, TimeUnit.SECONDS);
    await()
        .until(
            () ->
                store.listSync().size() == 1
                    && store.listSync().get(0).cacheSlotState == CacheSlotState.EVICT);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(store).size()).isEqualTo(1);
    CacheSlotMetadata evictedNode = store.getSync(hostname, name);
    assertThat(evictedNode.name).isEqualTo(name);
    assertThat(evictedNode.cacheSlotState)
        .isEqualTo(Metadata.CacheSlotMetadata.CacheSlotState.EVICT);
    assertThat(evictedNode.replicaId).isEqualTo(replicaId);
    assertThat(evictedNode.updatedTimeEpochMs).isGreaterThan(freeNode.updatedTimeEpochMs);
    assertThat(evictedNode.supportedIndexTypes).isEqualTo(SUPPORTED_INDEX_TYPES);
  }
}
