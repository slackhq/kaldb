package com.slack.kaldb.clusterManager;

import static com.slack.kaldb.clusterManager.ClusterHpaMetricService.CACHE_HPA_METRIC_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import brave.Tracing;
import com.slack.kaldb.metadata.cache.CacheSlotMetadata;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.hpa.HpaMetricMetadata;
import com.slack.kaldb.metadata.hpa.HpaMetricMetadataStore;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClusterHpaMetricServiceTest {

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;
  private AsyncCuratorFramework curatorFramework;

  private ReplicaMetadataStore replicaMetadataStore;
  private CacheSlotMetadataStore cacheSlotMetadataStore;

  private HpaMetricMetadataStore hpaMetricMetadataStore;

  @BeforeEach
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("ClusterHpaMetricServiceTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);

    replicaMetadataStore = spy(new ReplicaMetadataStore(curatorFramework, meterRegistry));
    cacheSlotMetadataStore = spy(new CacheSlotMetadataStore(curatorFramework, meterRegistry));
    hpaMetricMetadataStore = spy(new HpaMetricMetadataStore(curatorFramework, true, meterRegistry));
  }

  @AfterEach
  public void shutdown() throws IOException {
    hpaMetricMetadataStore.close();
    cacheSlotMetadataStore.close();
    replicaMetadataStore.close();
    curatorFramework.unwrap().close();

    testingServer.close();
    meterRegistry.close();
  }

  @Test
  void testLocking() throws Exception {
    ClusterHpaMetricService clusterHpaMetricService =
        new ClusterHpaMetricService(
            replicaMetadataStore, cacheSlotMetadataStore, hpaMetricMetadataStore);
    clusterHpaMetricService.CACHE_SCALEDOWN_LOCK = Duration.ofSeconds(2);

    assertThat(clusterHpaMetricService.tryCacheReplicasetLock("foo")).isTrue();
    assertThat(clusterHpaMetricService.tryCacheReplicasetLock("bar")).isFalse();
    assertThat(clusterHpaMetricService.tryCacheReplicasetLock("foo")).isTrue();
    Thread.sleep(2000);
    assertThat(clusterHpaMetricService.tryCacheReplicasetLock("bar")).isTrue();
    assertThat(clusterHpaMetricService.tryCacheReplicasetLock("bar")).isTrue();
    assertThat(clusterHpaMetricService.tryCacheReplicasetLock("foo")).isFalse();
  }

  @Test
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  void oneReplicasetScaledown() {
    ClusterHpaMetricService clusterHpaMetricService =
        new ClusterHpaMetricService(
            replicaMetadataStore, cacheSlotMetadataStore, hpaMetricMetadataStore);

    when(replicaMetadataStore.listSync())
        .thenReturn(
            List.of(
                new ReplicaMetadata(
                    "foo", "foo", "rep1", 1L, 0L, false, Metadata.IndexType.LOGS_LUCENE9),
                new ReplicaMetadata(
                    "bar", "bar", "rep2", 1L, 0L, false, Metadata.IndexType.LOGS_LUCENE9),
                new ReplicaMetadata(
                    "baz", "baz", "rep1", 1L, 0L, false, Metadata.IndexType.LOGS_LUCENE9)));

    when(cacheSlotMetadataStore.listSync())
        .thenReturn(
            List.of(
                new CacheSlotMetadata(
                    "foo",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    List.of(Metadata.IndexType.LOGS_LUCENE9),
                    "localhost",
                    "rep1"),
                new CacheSlotMetadata(
                    "bar",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    List.of(Metadata.IndexType.LOGS_LUCENE9),
                    "localhost",
                    "rep1"),
                new CacheSlotMetadata(
                    "baz",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    List.of(Metadata.IndexType.LOGS_LUCENE9),
                    "localhost",
                    "rep2"),
                new CacheSlotMetadata(
                    "bal",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    List.of(Metadata.IndexType.LOGS_LUCENE9),
                    "localhost",
                    "rep1")));

    clusterHpaMetricService.runOneIteration();

    AtomicReference<List<HpaMetricMetadata>> hpaMetricMetadataList = new AtomicReference<>();
    await()
        .until(
            () -> {
              hpaMetricMetadataList.set(hpaMetricMetadataStore.listSync());
              return hpaMetricMetadataList.get().size();
            },
            size -> size == 2);

    HpaMetricMetadata rep1Metadata =
        hpaMetricMetadataList.get().stream()
            .filter(
                metadata -> metadata.getName().equals(String.format(CACHE_HPA_METRIC_NAME, "rep1")))
            .findFirst()
            .get();

    HpaMetricMetadata rep2Metadata =
        hpaMetricMetadataList.get().stream()
            .filter(
                metadata -> metadata.getName().equals(String.format(CACHE_HPA_METRIC_NAME, "rep2")))
            .findFirst()
            .get();

    // 2 replicas, 3 slots
    assertThat(rep1Metadata.getValue()).isEqualTo(0.67);

    // 1 replica, 1 slot
    assertThat(rep2Metadata.getValue()).isEqualTo(1.0);
  }

  @Test
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  void twoReplicasetScaledown() {
    ClusterHpaMetricService clusterHpaMetricService =
        new ClusterHpaMetricService(
            replicaMetadataStore, cacheSlotMetadataStore, hpaMetricMetadataStore);

    when(replicaMetadataStore.listSync())
        .thenReturn(
            List.of(
                new ReplicaMetadata(
                    "foo", "foo", "rep1", 1L, 0L, false, Metadata.IndexType.LOGS_LUCENE9),
                new ReplicaMetadata(
                    "bar", "bar", "rep2", 1L, 0L, false, Metadata.IndexType.LOGS_LUCENE9)));

    when(cacheSlotMetadataStore.listSync())
        .thenReturn(
            List.of(
                new CacheSlotMetadata(
                    "foo",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    List.of(Metadata.IndexType.LOGS_LUCENE9),
                    "localhost",
                    "rep1"),
                new CacheSlotMetadata(
                    "bar",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    List.of(Metadata.IndexType.LOGS_LUCENE9),
                    "localhost",
                    "rep1"),
                new CacheSlotMetadata(
                    "baz",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    List.of(Metadata.IndexType.LOGS_LUCENE9),
                    "localhost",
                    "rep2"),
                new CacheSlotMetadata(
                    "bal",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    List.of(Metadata.IndexType.LOGS_LUCENE9),
                    "localhost",
                    "rep2")));

    clusterHpaMetricService.runOneIteration();

    AtomicReference<List<HpaMetricMetadata>> hpaMetricMetadataList = new AtomicReference<>();
    await()
        .until(
            () -> {
              hpaMetricMetadataList.set(hpaMetricMetadataStore.listSync());
              return hpaMetricMetadataList.get().size();
            },
            size -> size == 2);

    HpaMetricMetadata rep1Metadata =
        hpaMetricMetadataList.get().stream()
            .filter(
                metadata -> metadata.getName().equals(String.format(CACHE_HPA_METRIC_NAME, "rep1")))
            .findFirst()
            .get();

    HpaMetricMetadata rep2Metadata =
        hpaMetricMetadataList.get().stream()
            .filter(
                metadata -> metadata.getName().equals(String.format(CACHE_HPA_METRIC_NAME, "rep2")))
            .findFirst()
            .get();

    // only one should get a lock, the other should be 1 replica, 2 slots
    if (rep1Metadata.getValue().equals(1.0)) {
      assertThat(rep1Metadata.getValue()).isEqualTo(1.0);
      assertThat(rep2Metadata.getValue()).isEqualTo(0.5);
    } else {
      assertThat(rep1Metadata.getValue()).isEqualTo(0.5);
      assertThat(rep2Metadata.getValue()).isEqualTo(1.0);
    }
  }

  @Test
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  void twoReplicasetScaleup() {
    ClusterHpaMetricService clusterHpaMetricService =
        new ClusterHpaMetricService(
            replicaMetadataStore, cacheSlotMetadataStore, hpaMetricMetadataStore);

    when(replicaMetadataStore.listSync())
        .thenReturn(
            List.of(
                new ReplicaMetadata(
                    "foo", "foo", "rep1", 1L, 0L, false, Metadata.IndexType.LOGS_LUCENE9),
                new ReplicaMetadata(
                    "bar", "bar", "rep1", 1L, 0L, false, Metadata.IndexType.LOGS_LUCENE9),
                new ReplicaMetadata(
                    "baz", "bar", "rep2", 1L, 0L, false, Metadata.IndexType.LOGS_LUCENE9),
                new ReplicaMetadata(
                    "bal", "bar", "rep2", 1L, 0L, false, Metadata.IndexType.LOGS_LUCENE9)));

    when(cacheSlotMetadataStore.listSync())
        .thenReturn(
            List.of(
                new CacheSlotMetadata(
                    "foo",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    1,
                    List.of(Metadata.IndexType.LOGS_LUCENE9),
                    "localhost",
                    "rep1")));

    clusterHpaMetricService.runOneIteration();

    AtomicReference<List<HpaMetricMetadata>> hpaMetricMetadataList = new AtomicReference<>();
    await()
        .until(
            () -> {
              hpaMetricMetadataList.set(hpaMetricMetadataStore.listSync());
              return hpaMetricMetadataList.get().size();
            },
            size -> size == 2);

    HpaMetricMetadata rep1Metadata =
        hpaMetricMetadataList.get().stream()
            .filter(
                metadata -> metadata.getName().equals(String.format(CACHE_HPA_METRIC_NAME, "rep1")))
            .findFirst()
            .get();

    HpaMetricMetadata rep2Metadata =
        hpaMetricMetadataList.get().stream()
            .filter(
                metadata -> metadata.getName().equals(String.format(CACHE_HPA_METRIC_NAME, "rep2")))
            .findFirst()
            .get();

    // 2 replicas, 1 slot
    assertThat(rep1Metadata.getValue()).isEqualTo(2);

    // 2 replicas, 0 slots (will log an error and return a default no-op)
    assertThat(rep2Metadata.getValue()).isEqualTo(1);
  }

  @Test
  void testDemandFactorRounding() {
    assertThat(ClusterHpaMetricService.calculateDemandFactor(100, 98)).isEqualTo(0.98);
    assertThat(ClusterHpaMetricService.calculateDemandFactor(98, 100)).isEqualTo(1.03);
    assertThat(ClusterHpaMetricService.calculateDemandFactor(9999, 10000)).isEqualTo(1.01);
  }
}
