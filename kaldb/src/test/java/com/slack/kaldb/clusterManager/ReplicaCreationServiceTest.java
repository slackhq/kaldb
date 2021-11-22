package com.slack.kaldb.clusterManager;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.config.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.google.common.util.concurrent.Futures;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.MetricsUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReplicaCreationServiceTest {
  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  private MetadataStore metadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private ReplicaMetadataStore replicaMetadataStore;

  @Before
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("ReplicaCreatorServiceTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    metadataStore = ZookeeperMetadataStoreImpl.fromConfig(meterRegistry, zkConfig);
    snapshotMetadataStore = spy(new SnapshotMetadataStore(metadataStore, true));
    replicaMetadataStore = spy(new ReplicaMetadataStore(metadataStore, true));
  }

  @After
  public void shutdown() throws IOException {
    snapshotMetadataStore.close();
    replicaMetadataStore.close();
    metadataStore.close();

    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldDoNothingIfReplicasAlreadyExist() throws Exception {
    ReplicaMetadataStore replicaMetadataStore = new ReplicaMetadataStore(metadataStore, true);
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, true);

    SnapshotMetadata snapshotA =
        new SnapshotMetadata(
            "a", "a", Instant.now().toEpochMilli() - 1, Instant.now().toEpochMilli(), 0, "a");
    snapshotMetadataStore.createSync(snapshotA);

    replicaMetadataStore.createSync(
        ReplicaCreationService.replicaMetadataFromSnapshotId(snapshotA.snapshotId));
    replicaMetadataStore.createSync(
        ReplicaCreationService.replicaMetadataFromSnapshotId(snapshotA.snapshotId));
    await().until(() -> replicaMetadataStore.getCached().size() == 2);

    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setReplicasPerSnapshot(2)
            .setScheduleInitialDelayMins(0)
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore,
            snapshotMetadataStore,
            replicaCreationServiceConfig,
            replicaEvictionServiceConfig,
            meterRegistry);
    replicaCreationService.startAsync();
    replicaCreationService.awaitRunning(DEFAULT_START_STOP_DURATION);

    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(0);
    assertThat(replicaMetadataStore.getCached().size()).isEqualTo(2);

    replicaCreationService.stopAsync();
    replicaCreationService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void shouldCreateZeroReplicasNoneConfigured() throws Exception {
    SnapshotMetadata snapshotA =
        new SnapshotMetadata(
            "a", "a", Instant.now().toEpochMilli() - 1, Instant.now().toEpochMilli(), 0, "a");
    snapshotMetadataStore.createSync(snapshotA);

    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setReplicasPerSnapshot(0)
            .setScheduleInitialDelayMins(0)
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore,
            snapshotMetadataStore,
            replicaCreationServiceConfig,
            replicaEvictionServiceConfig,
            meterRegistry);
    replicaCreationService.startAsync();
    replicaCreationService.awaitRunning(DEFAULT_START_STOP_DURATION);

    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(0);
    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(0);
    assertThat(replicaMetadataStore.getCached().size()).isEqualTo(0);

    replicaCreationService.stopAsync();
    replicaCreationService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  // todo - make this a parameterized test once we upgrade to junit 5 to test various config
  // combinations
  public void shouldCreateFourReplicasIfNoneExist() throws Exception {
    SnapshotMetadata snapshotA =
        new SnapshotMetadata(
            "a", "a", Instant.now().toEpochMilli() - 1, Instant.now().toEpochMilli(), 0, "a");
    snapshotMetadataStore.createSync(snapshotA);

    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setReplicasPerSnapshot(4)
            .setScheduleInitialDelayMins(0)
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore,
            snapshotMetadataStore,
            replicaCreationServiceConfig,
            replicaEvictionServiceConfig,
            meterRegistry);
    replicaCreationService.startAsync();
    replicaCreationService.awaitRunning(DEFAULT_START_STOP_DURATION);

    await()
        .until(
            () ->
                MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry) == 4);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(4);
    await().until(() -> replicaMetadataStore.getCached().size() == 4);
    assertThat(
            (int)
                replicaMetadataStore
                    .getCached()
                    .stream()
                    .filter(replicaMetadata -> Objects.equals(replicaMetadata.snapshotId, "a"))
                    .count())
        .isEqualTo(4);

    replicaCreationService.stopAsync();
    replicaCreationService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void shouldHandleVeryLargeListOfIneligibleSnapshots() {
    int ineligibleSnapshotsToCreate = 500;
    int eligibleSnapshotsToCreate = 50;
    int replicasToCreate = 2;

    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setReplicasPerSnapshot(replicasToCreate)
            .setScheduleInitialDelayMins(0)
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    List<SnapshotMetadata> snapshotList = new ArrayList<>();
    IntStream.range(0, ineligibleSnapshotsToCreate)
        .forEach(
            (i) -> {
              String snapshotId = UUID.randomUUID().toString();
              SnapshotMetadata snapshot =
                  new SnapshotMetadata(
                      snapshotId,
                      snapshotId,
                      Instant.now().minus(1450, ChronoUnit.MINUTES).toEpochMilli(),
                      Instant.now().minus(1441, ChronoUnit.MINUTES).toEpochMilli(),
                      0,
                      snapshotId);
              snapshotList.add(snapshot);
            });

    List<SnapshotMetadata> eligibleSnapshots = new ArrayList<>();
    IntStream.range(0, eligibleSnapshotsToCreate)
        .forEach(
            (i) -> {
              String snapshotId = UUID.randomUUID().toString();
              SnapshotMetadata snapshot =
                  new SnapshotMetadata(
                      snapshotId,
                      snapshotId,
                      Instant.now().toEpochMilli() - 1,
                      Instant.now().toEpochMilli(),
                      0,
                      snapshotId);
              eligibleSnapshots.add(snapshot);
            });
    snapshotList.addAll(eligibleSnapshots);

    // randomize the order of eligible and ineligible snapshots and create them in parallel
    assertThat(snapshotList.size())
        .isEqualTo(eligibleSnapshotsToCreate + ineligibleSnapshotsToCreate);
    Collections.shuffle(snapshotList);
    snapshotList
        .parallelStream()
        .forEach((snapshotMetadata -> snapshotMetadataStore.createSync(snapshotMetadata)));
    List<SnapshotMetadata> snapshotMetadataList = snapshotMetadataStore.listSync();
    assertThat(snapshotMetadataList.size()).isEqualTo(snapshotList.size());

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore,
            snapshotMetadataStore,
            replicaCreationServiceConfig,
            replicaEvictionServiceConfig,
            meterRegistry);

    int replicasCreated = replicaCreationService.createReplicasForUnassignedSnapshots();
    int expectedReplicas = eligibleSnapshotsToCreate * replicasToCreate;

    await().until(() -> replicaMetadataStore.listSync().size() == expectedReplicas);
    await().until(() -> replicaMetadataStore.getCached().size() == expectedReplicas);

    assertThat(replicasCreated).isEqualTo(expectedReplicas);
    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(expectedReplicas);
    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_FAILED, meterRegistry))
        .isZero();
    assertThat(snapshotMetadataList).isEqualTo(snapshotMetadataStore.listSync());

    List<String> eligibleSnapshotIds =
        eligibleSnapshots
            .stream()
            .map(snapshotMetadata -> snapshotMetadata.snapshotId)
            .collect(Collectors.toList());
    assertTrue(
        replicaMetadataStore
            .listSync()
            .stream()
            .allMatch(
                (replicaMetadata) -> eligibleSnapshotIds.contains(replicaMetadata.snapshotId)));
  }

  @Test
  public void shouldCreateReplicaWhenSnapshotAddedAfterRunning() throws Exception {
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setReplicasPerSnapshot(2)
            .setScheduleInitialDelayMins(0)
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore,
            snapshotMetadataStore,
            replicaCreationServiceConfig,
            replicaEvictionServiceConfig,
            meterRegistry);
    replicaCreationService.startAsync();
    replicaCreationService.awaitRunning(DEFAULT_START_STOP_DURATION);

    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(0);
    assertThat(replicaMetadataStore.listSync().size()).isZero();
    assertThat(replicaMetadataStore.getCached().size()).isZero();

    // create a snapshot - we expect this to fire an event, and after the
    // EventAggregationSecs duration, attempt to create the replicas
    SnapshotMetadata snapshotA =
        new SnapshotMetadata(
            "a", "a", Instant.now().toEpochMilli() - 1, Instant.now().toEpochMilli(), 0, "a");
    snapshotMetadataStore.createSync(snapshotA);

    await().until(() -> replicaMetadataStore.getCached().size() == 2);
    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(2);
    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_FAILED, meterRegistry))
        .isZero();

    replicaCreationService.stopAsync();
    replicaCreationService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void shouldStillCreateReplicaIfFirstAttemptFails() throws Exception {
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setReplicasPerSnapshot(2)
            .setScheduleInitialDelayMins(0)
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore,
            snapshotMetadataStore,
            replicaCreationServiceConfig,
            replicaEvictionServiceConfig,
            meterRegistry);
    replicaCreationService.startAsync();
    replicaCreationService.awaitRunning(DEFAULT_START_STOP_DURATION);

    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(0);
    assertThat(replicaMetadataStore.listSync().size()).isZero();
    assertThat(replicaMetadataStore.getCached().size()).isZero();

    ExecutorService timeoutServiceExecutor = Executors.newSingleThreadExecutor();
    // allow the first replica creation to work, and timeout the second one
    doCallRealMethod()
        .doReturn(
            Futures.submit(
                () -> {
                  try {
                    Thread.sleep(30 * 1000);
                  } catch (InterruptedException ignored) {
                  }
                },
                timeoutServiceExecutor))
        .when(replicaMetadataStore)
        .create(any(ReplicaMetadata.class));

    // create a snapshot - we expect this to fire an event, and after the EventAggregationSecs
    // attempt to create the replicas
    SnapshotMetadata snapshotA =
        new SnapshotMetadata(
            "a", "a", Instant.now().toEpochMilli() - 1, Instant.now().toEpochMilli(), 0, "a");
    snapshotMetadataStore.createSync(snapshotA);

    await()
        .atMost(DEFAULT_ZK_TIMEOUT_SECS * 2, TimeUnit.SECONDS)
        .until(() -> replicaMetadataStore.getCached().size() == 1);
    await()
        .atMost(DEFAULT_ZK_TIMEOUT_SECS * 2, TimeUnit.SECONDS)
        .until(
            () ->
                MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry) == 1);
    await()
        .atMost(DEFAULT_ZK_TIMEOUT_SECS * 2, TimeUnit.SECONDS)
        .until(
            () -> MetricsUtil.getCount(ReplicaCreationService.REPLICAS_FAILED, meterRegistry) == 1);

    // reset the replica metdata store to work as expected
    doCallRealMethod().when(replicaMetadataStore).create(any(ReplicaMetadata.class));

    // manually trigger the next run and see if it creates the missing replica
    replicaCreationService.createReplicasForUnassignedSnapshots();

    await().until(() -> replicaMetadataStore.getCached().size() == 2);
    await()
        .atMost(DEFAULT_ZK_TIMEOUT_SECS * 2, TimeUnit.SECONDS)
        .until(
            () ->
                MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry) == 2);
    await()
        .atMost(DEFAULT_ZK_TIMEOUT_SECS * 2, TimeUnit.SECONDS)
        .until(
            () -> MetricsUtil.getCount(ReplicaCreationService.REPLICAS_FAILED, meterRegistry) == 1);

    replicaCreationService.stopAsync();
    replicaCreationService.awaitTerminated(DEFAULT_START_STOP_DURATION);

    timeoutServiceExecutor.shutdown();
  }

  @Test
  public void shouldHandleFailedCreateFutures() {
    SnapshotMetadata snapshotA =
        new SnapshotMetadata(
            "a", "a", Instant.now().toEpochMilli() - 1, Instant.now().toEpochMilli(), 0, "a");
    snapshotMetadataStore.createSync(snapshotA);
    await().until(() -> snapshotMetadataStore.getCached().size() == 1);

    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setReplicasPerSnapshot(2)
            .setScheduleInitialDelayMins(0)
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore,
            snapshotMetadataStore,
            replicaCreationServiceConfig,
            replicaEvictionServiceConfig,
            meterRegistry);

    ExecutorService timeoutServiceExecutor = Executors.newSingleThreadExecutor();
    // immediately fail the first replica create, timeout the second
    doReturn(Futures.immediateFailedFuture(new TimeoutException()))
        .doReturn(
            Futures.submit(
                () -> {
                  try {
                    Thread.sleep(30 * 1000);
                  } catch (InterruptedException ignored) {
                  }
                },
                timeoutServiceExecutor))
        .when(replicaMetadataStore)
        .create(any(ReplicaMetadata.class));

    int successfulReplicas = replicaCreationService.createReplicasForUnassignedSnapshots();
    assertThat(successfulReplicas).isZero();
    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_FAILED, meterRegistry))
        .isEqualTo(2);

    timeoutServiceExecutor.shutdown();
  }

  @Test
  public void shouldHandleMixOfSuccessfulFailedZkFutures() {
    SnapshotMetadata snapshotA =
        new SnapshotMetadata(
            "a", "a", Instant.now().toEpochMilli() - 1, Instant.now().toEpochMilli(), 0, "a");
    snapshotMetadataStore.createSync(snapshotA);
    await().until(() -> snapshotMetadataStore.getCached().size() == 1);

    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setReplicasPerSnapshot(2)
            .setScheduleInitialDelayMins(0)
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore,
            snapshotMetadataStore,
            replicaCreationServiceConfig,
            replicaEvictionServiceConfig,
            meterRegistry);

    ExecutorService timeoutServiceExecutor = Executors.newSingleThreadExecutor();
    // allow the first replica creation to work, and timeout the second one
    doCallRealMethod()
        .doReturn(
            Futures.submit(
                () -> {
                  try {
                    Thread.sleep(30 * 1000);
                  } catch (InterruptedException ignored) {
                  }
                },
                timeoutServiceExecutor))
        .when(replicaMetadataStore)
        .create(any(ReplicaMetadata.class));

    int successfulReplicas = replicaCreationService.createReplicasForUnassignedSnapshots();
    assertThat(successfulReplicas).isEqualTo(1);
    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(1);
    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_FAILED, meterRegistry))
        .isEqualTo(1);

    timeoutServiceExecutor.shutdown();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnInvalidAggregationSecs() {
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setEventAggregationSecs(0)
            .setReplicasPerSnapshot(2)
            .setScheduleInitialDelayMins(0)
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    new ReplicaCreationService(
        replicaMetadataStore,
        snapshotMetadataStore,
        replicaCreationServiceConfig,
        replicaEvictionServiceConfig,
        meterRegistry);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnInvalidLifespanMins() {
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setReplicasPerSnapshot(2)
            .setScheduleInitialDelayMins(0)
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(0)
            .build();

    new ReplicaCreationService(
        replicaMetadataStore,
        snapshotMetadataStore,
        replicaCreationServiceConfig,
        replicaEvictionServiceConfig,
        meterRegistry);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnInvalidReplicasPerSnapshot() {
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setReplicasPerSnapshot(-1)
            .setScheduleInitialDelayMins(0)
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    new ReplicaCreationService(
        replicaMetadataStore,
        snapshotMetadataStore,
        replicaCreationServiceConfig,
        replicaEvictionServiceConfig,
        meterRegistry);
  }
}
