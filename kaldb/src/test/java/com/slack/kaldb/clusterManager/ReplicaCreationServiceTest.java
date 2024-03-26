package com.slack.kaldb.clusterManager;

import static com.slack.kaldb.proto.metadata.Metadata.IndexType.LOGS_LUCENE9;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import brave.Tracing;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.core.KaldbMetadataTestUtils;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.MetricsUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ReplicaCreationServiceTest {
  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  private AsyncCuratorFramework curatorFramework;
  private SnapshotMetadataStore snapshotMetadataStore;
  private ReplicaMetadataStore replicaMetadataStore;

  @BeforeEach
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

    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    snapshotMetadataStore = spy(new SnapshotMetadataStore(curatorFramework, meterRegistry));
    replicaMetadataStore = spy(new ReplicaMetadataStore(curatorFramework, meterRegistry));
  }

  @AfterEach
  public void shutdown() throws IOException {
    snapshotMetadataStore.close();
    replicaMetadataStore.close();
    curatorFramework.unwrap().close();

    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldDoNothingIfReplicasAlreadyExist() throws Exception {
    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(curatorFramework, meterRegistry);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, meterRegistry);

    SnapshotMetadata snapshotA =
        new SnapshotMetadata(
            "a",
            "a",
            Instant.now().toEpochMilli() - 1,
            Instant.now().toEpochMilli(),
            0,
            "a",
            LOGS_LUCENE9);
    snapshotMetadataStore.createSync(snapshotA);

    replicaMetadataStore.createSync(
        ReplicaCreationService.replicaMetadataFromSnapshotId(
            snapshotA.snapshotId, "rep1", Instant.now().plusSeconds(60), false));
    replicaMetadataStore.createSync(
        ReplicaCreationService.replicaMetadataFromSnapshotId(
            snapshotA.snapshotId, "rep1", Instant.now().plusSeconds(60), false));
    await().until(() -> replicaMetadataStore.listSync().size() == 2);

    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .addAllReplicaSets(List.of("rep1", "rep2"))
            .setSchedulePeriodMins(10)
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setEventAggregationSecs(2)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore, snapshotMetadataStore, managerConfig, meterRegistry);
    replicaCreationService.startAsync();
    replicaCreationService.awaitRunning(DEFAULT_START_STOP_DURATION);

    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(0);
    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(2);
    assertThat(replicaMetadataStore.listSync().stream().filter(r -> r.isRestored).count()).isZero();

    replicaCreationService.stopAsync();
    replicaCreationService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void shouldCreateZeroReplicasNoneConfigured() throws Exception {
    SnapshotMetadata snapshotA =
        new SnapshotMetadata(
            "a",
            "a",
            Instant.now().toEpochMilli() - 1,
            Instant.now().toEpochMilli(),
            0,
            "a",
            LOGS_LUCENE9);
    snapshotMetadataStore.createSync(snapshotA);

    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .addAllReplicaSets(List.of())
            .setSchedulePeriodMins(10)
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setEventAggregationSecs(2)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore, snapshotMetadataStore, managerConfig, meterRegistry);
    replicaCreationService.startAsync();
    replicaCreationService.awaitRunning(DEFAULT_START_STOP_DURATION);

    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(0);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isEqualTo(0);
    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(0);
    assertThat(replicaMetadataStore.listSync().stream().filter(r -> r.isRestored).count()).isZero();

    replicaCreationService.stopAsync();
    replicaCreationService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void shouldCreateFourReplicasIfNoneExist() throws Exception {
    SnapshotMetadata snapshotA =
        new SnapshotMetadata(
            "a",
            "a",
            Instant.now().toEpochMilli() - 1,
            Instant.now().toEpochMilli(),
            0,
            "a",
            LOGS_LUCENE9);
    snapshotMetadataStore.createSync(snapshotA);

    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .addAllReplicaSets(List.of("rep1", "rep2", "rep3", "rep4"))
            .setSchedulePeriodMins(10)
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setEventAggregationSecs(2)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore, snapshotMetadataStore, managerConfig, meterRegistry);
    replicaCreationService.startAsync();
    replicaCreationService.awaitRunning(DEFAULT_START_STOP_DURATION);

    await()
        .until(
            () ->
                MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry) == 4);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isEqualTo(4);
    await().until(() -> replicaMetadataStore.listSync().size() == 4);
    assertThat(
            (int)
                replicaMetadataStore.listSync().stream()
                    .filter(replicaMetadata -> Objects.equals(replicaMetadata.snapshotId, "a"))
                    .count())
        .isEqualTo(4);
    assertThat(replicaMetadataStore.listSync().stream().filter(r -> r.isRestored).count()).isZero();

    replicaCreationService.stopAsync();
    replicaCreationService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void shouldNotCreateReplicasForLiveSnapshots() {
    SnapshotMetadata snapshotNotLive =
        new SnapshotMetadata(
            "a",
            "a",
            Instant.now().toEpochMilli() - 1,
            Instant.now().toEpochMilli(),
            0,
            "a",
            LOGS_LUCENE9);
    snapshotMetadataStore.createSync(snapshotNotLive);

    SnapshotMetadata snapshotLive =
        new SnapshotMetadata(
            "b",
            SnapshotMetadata.LIVE_SNAPSHOT_PATH,
            Instant.now().toEpochMilli() - 1,
            Instant.now().toEpochMilli(),
            0,
            "b",
            LOGS_LUCENE9);
    snapshotMetadataStore.createSync(snapshotLive);

    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .addAllReplicaSets(List.of("rep1"))
            .setSchedulePeriodMins(10)
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setEventAggregationSecs(2)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore, snapshotMetadataStore, managerConfig, meterRegistry);

    await()
        .until(
            () ->
                snapshotMetadataStore
                        .listSync()
                        .containsAll(Arrays.asList(snapshotLive, snapshotNotLive))
                    && snapshotMetadataStore.listSync().size() == 2);
    assertThat(replicaMetadataStore.listSync().isEmpty()).isTrue();

    Map<String, Integer> assignReplicas =
        replicaCreationService.createReplicasForUnassignedSnapshots();

    assertThat(assignReplicas.get("rep1")).isEqualTo(1);
    await().until(() -> replicaMetadataStore.listSync().size() == 1);

    assertThat(replicaMetadataStore.listSync().get(0).snapshotId)
        .isEqualTo(snapshotNotLive.snapshotId);

    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(1);
    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(
                ReplicaCreationService.REPLICA_ASSIGNMENT_TIMER, meterRegistry))
        .isEqualTo(1);
    assertThat(replicaMetadataStore.listSync().stream().filter(r -> r.isRestored).count()).isZero();
  }

  @Test
  public void shouldHandleVeryLargeListOfIneligibleSnapshots() {
    int ineligibleSnapshotsToCreate = 500;
    int liveSnapshotsToCreate = 30;
    int eligibleSnapshotsToCreate = 50;
    int replicasToCreate = 2;

    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .addAllReplicaSets(List.of("rep1", "rep2"))
            .setSchedulePeriodMins(10)
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setEventAggregationSecs(3)
            .setScheduleInitialDelayMins(0)
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
                      snapshotId,
                      LOGS_LUCENE9);
              snapshotList.add(snapshot);
            });

    IntStream.range(0, liveSnapshotsToCreate)
        .forEach(
            (i) -> {
              String snapshotId = UUID.randomUUID().toString();
              SnapshotMetadata snapshot =
                  new SnapshotMetadata(
                      snapshotId,
                      SnapshotMetadata.LIVE_SNAPSHOT_PATH,
                      Instant.now().toEpochMilli() - 1,
                      Instant.now().toEpochMilli(),
                      0,
                      snapshotId,
                      LOGS_LUCENE9);
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
                      snapshotId,
                      LOGS_LUCENE9);
              eligibleSnapshots.add(snapshot);
            });
    snapshotList.addAll(eligibleSnapshots);

    // randomize the order of eligible, ineligible, and live snapshots and create them in parallel
    assertThat(snapshotList.size())
        .isEqualTo(eligibleSnapshotsToCreate + ineligibleSnapshotsToCreate + liveSnapshotsToCreate);
    Collections.shuffle(snapshotList);
    snapshotList.parallelStream()
        .forEach(snapshotMetadata -> snapshotMetadataStore.createSync(snapshotMetadata));
    List<SnapshotMetadata> snapshotMetadataList =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(snapshotMetadataList.size()).isEqualTo(snapshotList.size());

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore, snapshotMetadataStore, managerConfig, meterRegistry);
    int expectedReplicas = eligibleSnapshotsToCreate * replicasToCreate;

    Map<String, Integer> replicasCreated =
        replicaCreationService.createReplicasForUnassignedSnapshots();
    assertThat(replicasCreated.values().stream().mapToInt(i -> i).sum())
        .isEqualTo(expectedReplicas);

    await()
        .until(
            () ->
                KaldbMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()
                    == expectedReplicas);
    await().until(() -> replicaMetadataStore.listSync().size() == expectedReplicas);

    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(expectedReplicas);
    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_FAILED, meterRegistry))
        .isZero();
    assertThat(snapshotMetadataList)
        .isEqualTo(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore));

    List<String> eligibleSnapshotIds =
        eligibleSnapshots.stream().map(snapshotMetadata -> snapshotMetadata.snapshotId).toList();
    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(replicaMetadataStore).stream()
                .allMatch(
                    (replicaMetadata) -> eligibleSnapshotIds.contains(replicaMetadata.snapshotId)))
        .isTrue();
    assertThat(replicaMetadataStore.listSync().stream().filter(r -> r.isRestored).count()).isZero();
  }

  @Test
  public void shouldCreateReplicaWhenSnapshotAddedAfterRunning() throws Exception {
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .addAllReplicaSets(List.of("rep1", "rep2"))
            .setSchedulePeriodMins(10)
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setEventAggregationSecs(2)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore, snapshotMetadataStore, managerConfig, meterRegistry);
    replicaCreationService.startAsync();
    replicaCreationService.awaitRunning(DEFAULT_START_STOP_DURATION);

    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(0);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isZero();
    assertThat(replicaMetadataStore.listSync().size()).isZero();

    // create a snapshot - we expect this to fire an event, and after the
    // EventAggregationSecs duration, attempt to create the replicas
    SnapshotMetadata snapshotA =
        new SnapshotMetadata(
            "a",
            "a",
            Instant.now().toEpochMilli() - 1,
            Instant.now().toEpochMilli(),
            0,
            "a",
            LOGS_LUCENE9);
    snapshotMetadataStore.createSync(snapshotA);

    await().until(() -> replicaMetadataStore.listSync().size() == 2);
    assertThat(replicaMetadataStore.listSync().stream().filter(r -> r.isRestored).count()).isZero();
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
            .addAllReplicaSets(List.of("rep1", "rep2"))
            .setSchedulePeriodMins(10)
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setEventAggregationSecs(2)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore, snapshotMetadataStore, managerConfig, meterRegistry);
    replicaCreationService.futuresListTimeoutSecs = 2;
    replicaCreationService.startAsync();
    replicaCreationService.awaitRunning(DEFAULT_START_STOP_DURATION);

    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(0);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isZero();
    assertThat(replicaMetadataStore.listSync().size()).isZero();

    ExecutorService timeoutServiceExecutor = Executors.newSingleThreadExecutor();

    AsyncStage asyncStage = mock(AsyncStage.class);
    when(asyncStage.toCompletableFuture())
        .thenReturn(
            CompletableFuture.runAsync(
                () -> {
                  try {
                    Thread.sleep(30 * 1000);
                  } catch (InterruptedException ignored) {
                  }
                },
                timeoutServiceExecutor));

    // allow the first replica creation to work, and timeout the second one
    doCallRealMethod()
        .doReturn(asyncStage)
        .when(replicaMetadataStore)
        .createAsync(any(ReplicaMetadata.class));

    // create a snapshot - we expect this to fire an event, and after the EventAggregationSecs
    // attempt to create the replicas
    SnapshotMetadata snapshotA =
        new SnapshotMetadata(
            "a",
            "a",
            Instant.now().toEpochMilli() - 1,
            Instant.now().toEpochMilli(),
            0,
            "a",
            LOGS_LUCENE9);
    snapshotMetadataStore.createSync(snapshotA);

    await()
        .atMost(replicaCreationService.futuresListTimeoutSecs * 2L, TimeUnit.SECONDS)
        .until(() -> replicaMetadataStore.listSync().size() == 1);
    await()
        .atMost(replicaCreationService.futuresListTimeoutSecs * 2L, TimeUnit.SECONDS)
        .until(
            () ->
                MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry) == 1);
    await()
        .atMost(replicaCreationService.futuresListTimeoutSecs * 2L, TimeUnit.SECONDS)
        .until(
            () -> MetricsUtil.getCount(ReplicaCreationService.REPLICAS_FAILED, meterRegistry) == 1);

    // reset the replica metadata store to work as expected
    doCallRealMethod().when(replicaMetadataStore).createAsync(any(ReplicaMetadata.class));

    // manually trigger the next run and see if it creates the missing replica
    replicaCreationService.createReplicasForUnassignedSnapshots();

    await().until(() -> replicaMetadataStore.listSync().size() == 2);
    assertThat(replicaMetadataStore.listSync().stream().filter(r -> r.isRestored).count()).isZero();
    await()
        .atMost(replicaCreationService.futuresListTimeoutSecs * 2L, TimeUnit.SECONDS)
        .until(
            () ->
                MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry) == 2);
    await()
        .atMost(replicaCreationService.futuresListTimeoutSecs * 2L, TimeUnit.SECONDS)
        .until(
            () -> MetricsUtil.getCount(ReplicaCreationService.REPLICAS_FAILED, meterRegistry) == 1);

    replicaCreationService.stopAsync();
    replicaCreationService.awaitTerminated(DEFAULT_START_STOP_DURATION);

    timeoutServiceExecutor.shutdown();
    //noinspection ResultOfMethodCallIgnored
    timeoutServiceExecutor.awaitTermination(15, TimeUnit.SECONDS);
  }

  @Test
  public void shouldHandleFailedCreateFutures() {
    SnapshotMetadata snapshotA =
        new SnapshotMetadata(
            "a",
            "a",
            Instant.now().toEpochMilli() - 1,
            Instant.now().toEpochMilli(),
            0,
            "a",
            LOGS_LUCENE9);
    snapshotMetadataStore.createSync(snapshotA);
    await().until(() -> snapshotMetadataStore.listSync().size() == 1);

    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .addAllReplicaSets(List.of("rep1", "rep2"))
            .setSchedulePeriodMins(10)
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore, snapshotMetadataStore, managerConfig, meterRegistry);

    AsyncStage asyncStage = mock(AsyncStage.class);
    when(asyncStage.toCompletableFuture())
        .thenReturn(CompletableFuture.failedFuture(new Exception()));

    doCallRealMethod().doReturn(asyncStage).when(replicaMetadataStore).createAsync(any());

    Map<String, Integer> successfulReplicas =
        replicaCreationService.createReplicasForUnassignedSnapshots();
    assertThat(successfulReplicas.get("rep1")).isEqualTo(1);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isEqualTo(1);
    assertThat(replicaMetadataStore.listSync().stream().filter(r -> r.isRestored).count()).isZero();
    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(1);
    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_FAILED, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleMixOfSuccessfulFailedZkFutures() {
    SnapshotMetadata snapshotA =
        new SnapshotMetadata(
            "a",
            "a",
            Instant.now().toEpochMilli() - 1,
            Instant.now().toEpochMilli(),
            0,
            "a",
            LOGS_LUCENE9);
    snapshotMetadataStore.createSync(snapshotA);
    await().until(() -> snapshotMetadataStore.listSync().size() == 1);

    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .addAllReplicaSets(List.of("rep1", "rep2"))
            .setSchedulePeriodMins(10)
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setEventAggregationSecs(2)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore, snapshotMetadataStore, managerConfig, meterRegistry);
    replicaCreationService.futuresListTimeoutSecs = 2;

    ExecutorService timeoutServiceExecutor = Executors.newSingleThreadExecutor();

    AsyncStage asyncStage = mock(AsyncStage.class);
    when(asyncStage.toCompletableFuture())
        .thenReturn(
            CompletableFuture.runAsync(
                () -> {
                  try {
                    Thread.sleep(30 * 1000);
                  } catch (InterruptedException ignored) {
                  }
                },
                timeoutServiceExecutor));

    // allow the first replica creation to work, and timeout the second one
    doCallRealMethod()
        .doReturn(asyncStage)
        .when(replicaMetadataStore)
        .createAsync(any(ReplicaMetadata.class));

    Map<String, Integer> successfulReplicas =
        replicaCreationService.createReplicasForUnassignedSnapshots();
    assertThat(successfulReplicas.get("rep1")).isEqualTo(1);
    assertThat(replicaMetadataStore.listSync().stream().filter(r -> r.isRestored).count()).isZero();
    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(1);
    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_FAILED, meterRegistry))
        .isEqualTo(1);

    timeoutServiceExecutor.shutdown();
  }

  @Test
  public void shouldThrowOnInvalidAggregationSecs() {
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .addAllReplicaSets(List.of("rep1", "rep2"))
            .setSchedulePeriodMins(10)
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setEventAggregationSecs(0)
            .setScheduleInitialDelayMins(0)
            .build();

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                new ReplicaCreationService(
                    replicaMetadataStore, snapshotMetadataStore, managerConfig, meterRegistry));
  }

  @Test
  public void shouldThrowOnInvalidLifespanMins() {
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .addAllReplicaSets(List.of("rep1", "rep2"))
            .setSchedulePeriodMins(10)
            .setReplicaLifespanMins(0)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setEventAggregationSecs(2)
            .setScheduleInitialDelayMins(0)
            .build();

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                new ReplicaCreationService(
                    replicaMetadataStore, snapshotMetadataStore, managerConfig, meterRegistry));
  }
}
