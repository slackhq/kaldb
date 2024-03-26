package com.slack.kaldb.clusterManager;

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
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.testlib.MetricsUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class RecoveryTaskAssignmentServiceTest {

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;
  private AsyncCuratorFramework curatorFramework;
  private RecoveryTaskMetadataStore recoveryTaskMetadataStore;
  private RecoveryNodeMetadataStore recoveryNodeMetadataStore;

  @BeforeEach
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("RecoveryTaskAssignmentServiceTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    recoveryTaskMetadataStore =
        spy(new RecoveryTaskMetadataStore(curatorFramework, true, meterRegistry));
    recoveryNodeMetadataStore =
        spy(new RecoveryNodeMetadataStore(curatorFramework, true, meterRegistry));
  }

  @AfterEach
  public void shutdown() throws IOException {
    recoveryNodeMetadataStore.close();
    recoveryTaskMetadataStore.close();
    curatorFramework.unwrap().close();

    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldCheckInvalidEventAggregation() {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setSchedulePeriodMins(10)
                .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setRecoveryTaskAssignmentServiceConfig(recoveryTaskAssignmentServiceConfig)
            .setScheduleInitialDelayMins(1)
            .setEventAggregationSecs(-1)
            .build();

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                new RecoveryTaskAssignmentService(
                    recoveryTaskMetadataStore,
                    recoveryNodeMetadataStore,
                    managerConfig,
                    meterRegistry));
  }

  @Test
  public void shouldCheckInvalidPeriod() {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setSchedulePeriodMins(-1)
                .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setRecoveryTaskAssignmentServiceConfig(recoveryTaskAssignmentServiceConfig)
            .setScheduleInitialDelayMins(1)
            .setEventAggregationSecs(1)
            .build();

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                new RecoveryTaskAssignmentService(
                        recoveryTaskMetadataStore,
                        recoveryNodeMetadataStore,
                        managerConfig,
                        meterRegistry)
                    .scheduler());
  }

  @Test
  public void shouldHandleNoNodesOrTasks() {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setSchedulePeriodMins(10)
                .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setRecoveryTaskAssignmentServiceConfig(recoveryTaskAssignmentServiceConfig)
            .setScheduleInitialDelayMins(5)
            .setEventAggregationSecs(1)
            .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore, recoveryNodeMetadataStore, managerConfig, meterRegistry);

    int assignments = recoveryTaskAssignmentService.assignRecoveryTasksToNodes();

    assertThat(assignments).isEqualTo(0);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).isEmpty())
        .isTrue();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore).isEmpty())
        .isTrue();

    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNMENT_FAILURES, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(
                RecoveryTaskAssignmentService.RECOVERY_TASK_ASSIGNMENT_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleNoAvailableNodes() {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setSchedulePeriodMins(10)
                .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setRecoveryTaskAssignmentServiceConfig(recoveryTaskAssignmentServiceConfig)
            .setScheduleInitialDelayMins(5)
            .setEventAggregationSecs(1)
            .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore, recoveryNodeMetadataStore, managerConfig, meterRegistry);

    for (int i = 0; i < 3; i++) {
      recoveryTaskMetadataStore.createAsync(
          new RecoveryTaskMetadata(
              UUID.randomUUID().toString(), "1", 0, 1, Instant.now().toEpochMilli()));
    }

    await().until(() -> recoveryTaskMetadataStore.listSync().size() == 3);

    int assignments = recoveryTaskAssignmentService.assignRecoveryTasksToNodes();

    assertThat(assignments).isEqualTo(0);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).size())
        .isEqualTo(3);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore).isEmpty())
        .isTrue();

    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNMENT_FAILURES, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(3);
    assertThat(
            MetricsUtil.getTimerCount(
                RecoveryTaskAssignmentService.RECOVERY_TASK_ASSIGNMENT_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleMixOfNodeStates() {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setSchedulePeriodMins(10)
                .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setRecoveryTaskAssignmentServiceConfig(recoveryTaskAssignmentServiceConfig)
            .setScheduleInitialDelayMins(5)
            .setEventAggregationSecs(1)
            .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore, recoveryNodeMetadataStore, managerConfig, meterRegistry);

    for (int i = 0; i < 3; i++) {
      recoveryTaskMetadataStore.createAsync(
          new RecoveryTaskMetadata(
              UUID.randomUUID().toString(), "1", 0, 1, Instant.now().toEpochMilli()));
    }

    recoveryNodeMetadataStore.createAsync(
        new RecoveryNodeMetadata(
            UUID.randomUUID().toString(),
            Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
            "",
            Instant.now().toEpochMilli()));

    List<RecoveryNodeMetadata> ineligibleRecoveryNodes = new ArrayList<>();
    RecoveryNodeMetadata ineligibleAssigned =
        new RecoveryNodeMetadata(
            UUID.randomUUID().toString(),
            Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED,
            "123",
            Instant.now().toEpochMilli());
    ineligibleRecoveryNodes.add(ineligibleAssigned);
    recoveryNodeMetadataStore.createAsync(ineligibleAssigned);

    RecoveryNodeMetadata ineligibleRecovering =
        new RecoveryNodeMetadata(
            UUID.randomUUID().toString(),
            Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING,
            "321",
            Instant.now().toEpochMilli());
    ineligibleRecoveryNodes.add(ineligibleRecovering);
    recoveryNodeMetadataStore.createAsync(ineligibleRecovering);

    await().until(() -> recoveryNodeMetadataStore.listSync().size() == 3);
    await().until(() -> recoveryTaskMetadataStore.listSync().size() == 3);

    int assignments = recoveryTaskAssignmentService.assignRecoveryTasksToNodes();

    assertThat(assignments).isEqualTo(1);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).size())
        .isEqualTo(3);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore).size())
        .isEqualTo(3);
    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore)
                .containsAll(ineligibleRecoveryNodes))
        .isTrue();
    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore).stream()
                .filter(
                    recoveryNodeMetadata ->
                        recoveryNodeMetadata.recoveryNodeState.equals(
                            Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED))
                .count())
        .isEqualTo(2);

    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNMENT_FAILURES, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(2);
    assertThat(
            MetricsUtil.getTimerCount(
                RecoveryTaskAssignmentService.RECOVERY_TASK_ASSIGNMENT_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleNoAvailableTasks() {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setSchedulePeriodMins(10)
                .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setRecoveryTaskAssignmentServiceConfig(recoveryTaskAssignmentServiceConfig)
            .setScheduleInitialDelayMins(5)
            .setEventAggregationSecs(1)
            .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore, recoveryNodeMetadataStore, managerConfig, meterRegistry);

    for (int i = 0; i < 3; i++) {
      recoveryNodeMetadataStore.createAsync(
          new RecoveryNodeMetadata(
              UUID.randomUUID().toString(),
              Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
              "",
              Instant.now().toEpochMilli()));
    }

    await().until(() -> recoveryNodeMetadataStore.listSync().size() == 3);

    int assignments = recoveryTaskAssignmentService.assignRecoveryTasksToNodes();

    assertThat(assignments).isEqualTo(0);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).isEmpty())
        .isTrue();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore).size())
        .isEqualTo(3);

    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNMENT_FAILURES, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(
                RecoveryTaskAssignmentService.RECOVERY_TASK_ASSIGNMENT_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldAssignOldestTasksFirst() {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setSchedulePeriodMins(10)
                .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setRecoveryTaskAssignmentServiceConfig(recoveryTaskAssignmentServiceConfig)
            .setScheduleInitialDelayMins(5)
            .setEventAggregationSecs(1)
            .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore, recoveryNodeMetadataStore, managerConfig, meterRegistry);

    RecoveryTaskMetadata newTask =
        new RecoveryTaskMetadata(
            UUID.randomUUID().toString(), "1", 0, 1, Instant.now().toEpochMilli());
    recoveryTaskMetadataStore.createAsync(newTask);

    RecoveryTaskMetadata oldTask =
        new RecoveryTaskMetadata(
            UUID.randomUUID().toString(),
            "1",
            0,
            1,
            Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli());
    recoveryTaskMetadataStore.createAsync(oldTask);

    recoveryNodeMetadataStore.createAsync(
        new RecoveryNodeMetadata(
            UUID.randomUUID().toString(),
            Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
            "",
            Instant.now().toEpochMilli()));

    await().until(() -> recoveryNodeMetadataStore.listSync().size() == 1);
    await().until(() -> recoveryTaskMetadataStore.listSync().size() == 2);

    int assignments = recoveryTaskAssignmentService.assignRecoveryTasksToNodes();

    assertThat(assignments).isEqualTo(1);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskMetadataStore).size())
        .isEqualTo(2);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore).size())
        .isEqualTo(1);
    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore)
                .get(0)
                .recoveryTaskName)
        .isEqualTo(oldTask.name);
    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore)
                .get(0)
                .recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED);

    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNMENT_FAILURES, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(
                RecoveryTaskAssignmentService.RECOVERY_TASK_ASSIGNMENT_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldRetryFailedAssignmentOnFollowingRun() {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setSchedulePeriodMins(10)
                .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setRecoveryTaskAssignmentServiceConfig(recoveryTaskAssignmentServiceConfig)
            .setScheduleInitialDelayMins(5)
            .setEventAggregationSecs(1)
            .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore, recoveryNodeMetadataStore, managerConfig, meterRegistry);

    for (int i = 0; i < 3; i++) {
      recoveryNodeMetadataStore.createAsync(
          new RecoveryNodeMetadata(
              UUID.randomUUID().toString(),
              Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
              "",
              Instant.now().toEpochMilli()));
    }

    for (int i = 0; i < 3; i++) {
      recoveryTaskMetadataStore.createAsync(
          new RecoveryTaskMetadata(
              UUID.randomUUID().toString(), "1", 0, 1, Instant.now().toEpochMilli()));
    }

    await().until(() -> recoveryTaskMetadataStore.listSync().size() == 3);
    await().until(() -> recoveryNodeMetadataStore.listSync().size() == 3);

    AsyncStage asyncStage = mock(AsyncStage.class);
    when(asyncStage.toCompletableFuture())
        .thenReturn(CompletableFuture.failedFuture(new Exception()));

    doCallRealMethod()
        .doCallRealMethod()
        .doReturn(asyncStage)
        .when(recoveryNodeMetadataStore)
        .updateAsync(any());

    int firstAttemptAssignment = recoveryTaskAssignmentService.assignRecoveryTasksToNodes();
    assertThat(firstAttemptAssignment).isEqualTo(2);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNED, meterRegistry))
        .isEqualTo(2);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNMENT_FAILURES, meterRegistry))
        .isEqualTo(1);

    await()
        .until(
            () ->
                recoveryNodeMetadataStore.listSync().stream()
                        .filter(
                            recoveryNodeMetadata ->
                                recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED))
                        .count()
                    == 2);

    doCallRealMethod().when(recoveryNodeMetadataStore).updateAsync(any());

    int secondAttemptAssignment = recoveryTaskAssignmentService.assignRecoveryTasksToNodes();
    assertThat(secondAttemptAssignment).isEqualTo(1);

    await()
        .until(
            () ->
                recoveryNodeMetadataStore.listSync().stream()
                        .filter(
                            recoveryNodeMetadata ->
                                recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED))
                        .count()
                    == 3);

    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNED, meterRegistry))
        .isEqualTo(3);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNMENT_FAILURES, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(
                RecoveryTaskAssignmentService.RECOVERY_TASK_ASSIGNMENT_TIMER, meterRegistry))
        .isEqualTo(2);
  }

  @Test
  public void shouldHandleTimedOutFutures() {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setSchedulePeriodMins(10)
                .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setRecoveryTaskAssignmentServiceConfig(recoveryTaskAssignmentServiceConfig)
            .setScheduleInitialDelayMins(5)
            .setEventAggregationSecs(1)
            .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore, recoveryNodeMetadataStore, managerConfig, meterRegistry);
    recoveryTaskAssignmentService.futuresListTimeoutSecs = 2;

    for (int i = 0; i < 2; i++) {
      RecoveryNodeMetadata recoveryNodeMetadata =
          new RecoveryNodeMetadata(
              UUID.randomUUID().toString(),
              Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
              "",
              Instant.now().toEpochMilli());
      recoveryNodeMetadataStore.createAsync(recoveryNodeMetadata);
    }

    for (int i = 0; i < 2; i++) {
      recoveryTaskMetadataStore.createAsync(
          new RecoveryTaskMetadata(
              UUID.randomUUID().toString(), "1", 0, 1, Instant.now().toEpochMilli()));
    }

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

    doCallRealMethod().doReturn(asyncStage).when(recoveryNodeMetadataStore).updateAsync(any());

    await().until(() -> recoveryNodeMetadataStore.listSync().size() == 2);
    await().until(() -> recoveryTaskMetadataStore.listSync().size() == 2);

    int assignments = recoveryTaskAssignmentService.assignRecoveryTasksToNodes();

    assertThat(assignments).isEqualTo(1);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNMENT_FAILURES, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(
                RecoveryTaskAssignmentService.RECOVERY_TASK_ASSIGNMENT_TIMER, meterRegistry))
        .isEqualTo(1);

    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore).stream()
                .filter(
                    recoveryNodeMetadata ->
                        recoveryNodeMetadata.recoveryNodeState.equals(
                            Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED))
                .count())
        .isEqualTo(1);
    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore).stream()
                .filter(
                    recoveryNodeMetadata ->
                        recoveryNodeMetadata.recoveryNodeState.equals(
                            Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE))
                .count())
        .isEqualTo(1);

    timeoutServiceExecutor.shutdown();
  }

  @Test
  public void shouldHandleExceptionalFutures() {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setSchedulePeriodMins(10)
                .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setRecoveryTaskAssignmentServiceConfig(recoveryTaskAssignmentServiceConfig)
            .setScheduleInitialDelayMins(5)
            .setEventAggregationSecs(1)
            .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore, recoveryNodeMetadataStore, managerConfig, meterRegistry);

    for (int i = 0; i < 2; i++) {
      RecoveryNodeMetadata recoveryNodeMetadata =
          new RecoveryNodeMetadata(
              UUID.randomUUID().toString(),
              Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
              "",
              Instant.now().toEpochMilli());
      recoveryNodeMetadataStore.createAsync(recoveryNodeMetadata);
    }

    for (int i = 0; i < 2; i++) {
      recoveryTaskMetadataStore.createAsync(
          new RecoveryTaskMetadata(
              UUID.randomUUID().toString(), "1", 0, 1, Instant.now().toEpochMilli()));
    }

    AsyncStage asyncStage = mock(AsyncStage.class);
    when(asyncStage.toCompletableFuture())
        .thenReturn(CompletableFuture.failedFuture(new Exception()));

    doCallRealMethod().doReturn(asyncStage).when(recoveryNodeMetadataStore).updateAsync(any());

    await().until(() -> recoveryNodeMetadataStore.listSync().size() == 2);
    await().until(() -> recoveryTaskMetadataStore.listSync().size() == 2);

    int assignments = recoveryTaskAssignmentService.assignRecoveryTasksToNodes();

    assertThat(assignments).isEqualTo(1);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_ASSIGNMENT_FAILURES, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(
                RecoveryTaskAssignmentService.RECOVERY_TASK_ASSIGNMENT_TIMER, meterRegistry))
        .isEqualTo(1);

    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore).stream()
                .filter(
                    recoveryNodeMetadata ->
                        recoveryNodeMetadata.recoveryNodeState.equals(
                            Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED))
                .count())
        .isEqualTo(1);
    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(recoveryNodeMetadataStore).stream()
                .filter(
                    recoveryNodeMetadata ->
                        recoveryNodeMetadata.recoveryNodeState.equals(
                            Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE))
                .count())
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleNodesAvailableFirstLifecycle() throws Exception {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setSchedulePeriodMins(10)
                .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setRecoveryTaskAssignmentServiceConfig(recoveryTaskAssignmentServiceConfig)
            .setScheduleInitialDelayMins(1)
            .setEventAggregationSecs(2)
            .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore, recoveryNodeMetadataStore, managerConfig, meterRegistry);
    recoveryTaskAssignmentService.startAsync();
    recoveryTaskAssignmentService.awaitRunning(DEFAULT_START_STOP_DURATION);

    for (int i = 0; i < 3; i++) {
      recoveryNodeMetadataStore.createAsync(
          new RecoveryNodeMetadata(
              UUID.randomUUID().toString(),
              Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
              "",
              Instant.now().toEpochMilli()));
    }

    await().until(() -> recoveryNodeMetadataStore.listSync().size() == 3);
    // all nodes should be FREE, and have no assignment
    await()
        .until(
            () ->
                recoveryNodeMetadataStore.listSync().stream()
                    .allMatch(
                        (recoveryNodeMetadata) ->
                            recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE)
                                && recoveryNodeMetadata.recoveryTaskName.isEmpty()));

    for (int i = 0; i < 10; i++) {
      recoveryTaskMetadataStore.createAsync(
          new RecoveryTaskMetadata(
              UUID.randomUUID().toString(), "1", 0, 1, Instant.now().toEpochMilli()));
    }

    await().until(() -> recoveryTaskMetadataStore.listSync().size() == 10);
    // all nodes should be ASSIGNED, and have an assignment
    await()
        .until(
            () ->
                recoveryNodeMetadataStore.listSync().stream()
                    .allMatch(
                        (recoveryNodeMetadata) ->
                            recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED)
                                && !recoveryNodeMetadata.recoveryTaskName.isEmpty()));

    // mark all as recovering
    recoveryNodeMetadataStore
        .listSync()
        .forEach(
            recoveryNodeMetadata -> {
              RecoveryNodeMetadata updatedRecoveryNode =
                  new RecoveryNodeMetadata(
                      recoveryNodeMetadata.name,
                      Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING,
                      recoveryNodeMetadata.recoveryTaskName,
                      Instant.now().toEpochMilli());
              recoveryNodeMetadataStore.updateAsync(updatedRecoveryNode);
            });

    // all nodes should be recovering
    await()
        .until(
            () ->
                recoveryNodeMetadataStore.listSync().stream()
                    .allMatch(
                        (recoveryNodeMetadata) ->
                            recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING)
                                && !recoveryNodeMetadata.recoveryTaskName.isEmpty()));

    Instant before = Instant.now();
    // next delete the task, and mark the node as free
    recoveryNodeMetadataStore
        .listSync()
        .forEach(
            recoveryNodeMetadata -> {
              // delete the task
              recoveryTaskMetadataStore.deleteSync(recoveryNodeMetadata.recoveryTaskName);
              // free this node
              RecoveryNodeMetadata updatedRecoveryNode =
                  new RecoveryNodeMetadata(
                      recoveryNodeMetadata.name,
                      Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
                      "",
                      Instant.now().toEpochMilli());
              recoveryNodeMetadataStore.updateAsync(updatedRecoveryNode);
            });

    // wait until all nodes have been re-assigned to new tasks
    await()
        .until(
            () ->
                recoveryNodeMetadataStore.listSync().stream()
                    .allMatch(
                        recoveryNodeMetadata ->
                            recoveryNodeMetadata.updatedTimeEpochMs > before.toEpochMilli()
                                && recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED)));
    assertThat(recoveryTaskMetadataStore.listSync().size()).isEqualTo(7);

    recoveryTaskAssignmentService.stopAsync();
    recoveryTaskAssignmentService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  @Disabled // Flakey, occasionally throws InternalMetadataStore on the recoveryNodeMetadataStore
  public void shouldHandleTasksAvailableFirstLifecycle() throws Exception {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setSchedulePeriodMins(10)
                .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setRecoveryTaskAssignmentServiceConfig(recoveryTaskAssignmentServiceConfig)
            .setScheduleInitialDelayMins(2)
            .setEventAggregationSecs(1)
            .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore, recoveryNodeMetadataStore, managerConfig, meterRegistry);
    recoveryTaskAssignmentService.startAsync();
    recoveryTaskAssignmentService.awaitRunning(DEFAULT_START_STOP_DURATION);

    for (int i = 0; i < 10; i++) {
      recoveryTaskMetadataStore.createAsync(
          new RecoveryTaskMetadata(
              UUID.randomUUID().toString(), "1", 0, 1, Instant.now().toEpochMilli()));
    }
    await().until(() -> recoveryTaskMetadataStore.listSync().size() == 10);

    for (int i = 0; i < 3; i++) {
      recoveryNodeMetadataStore.createAsync(
          new RecoveryNodeMetadata(
              UUID.randomUUID().toString(),
              Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
              "",
              Instant.now().toEpochMilli()));
    }
    await().until(() -> recoveryNodeMetadataStore.listSync().size() == 3);

    // all nodes should immediately pickup tasks and have an assignment
    await()
        .until(
            () ->
                recoveryNodeMetadataStore.listSync().stream()
                    .allMatch(
                        (recoveryNodeMetadata) ->
                            recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED)
                                && !recoveryNodeMetadata.recoveryTaskName.isEmpty()));

    // mark all as recovering
    recoveryNodeMetadataStore
        .listSync()
        .forEach(
            recoveryNodeMetadata -> {
              RecoveryNodeMetadata updatedRecoveryNode =
                  new RecoveryNodeMetadata(
                      recoveryNodeMetadata.name,
                      Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING,
                      recoveryNodeMetadata.recoveryTaskName,
                      Instant.now().toEpochMilli());
              recoveryNodeMetadataStore.updateAsync(updatedRecoveryNode);
            });

    // all nodes should be recovering
    await()
        .until(
            () ->
                recoveryNodeMetadataStore.listSync().stream()
                    .allMatch(
                        (recoveryNodeMetadata) ->
                            recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING)
                                && !recoveryNodeMetadata.recoveryTaskName.isEmpty()));

    Instant before = Instant.now();
    // next delete the task, and mark the node as free
    recoveryNodeMetadataStore
        .listSync()
        .forEach(
            recoveryNodeMetadata -> {
              // delete the task
              recoveryTaskMetadataStore.deleteSync(recoveryNodeMetadata.recoveryTaskName);
              // free this node
              RecoveryNodeMetadata updatedRecoveryNode =
                  new RecoveryNodeMetadata(
                      recoveryNodeMetadata.name,
                      Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
                      "",
                      Instant.now().toEpochMilli());
              recoveryNodeMetadataStore.updateAsync(updatedRecoveryNode);
            });

    // wait until all nodes have been re-assigned to new tasks
    await()
        .until(
            () ->
                recoveryNodeMetadataStore.listSync().stream()
                    .allMatch(
                        recoveryNodeMetadata ->
                            recoveryNodeMetadata.updatedTimeEpochMs > before.toEpochMilli()
                                && recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED)));
    assertThat(recoveryTaskMetadataStore.listSync().size()).isEqualTo(7);

    recoveryTaskAssignmentService.stopAsync();
    recoveryTaskAssignmentService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }
}
