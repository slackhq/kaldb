package com.slack.kaldb.clusterManager;

import static com.slack.kaldb.proto.metadata.Metadata.IndexType.LOGS_LUCENE9;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.fail;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.MetricsUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.naming.SizeLimitExceededException;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ReplicaRestoreServiceTest {

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;
  private AsyncCuratorFramework curatorFramework;
  private KaldbConfigs.ManagerConfig managerConfig;
  private ReplicaMetadataStore replicaMetadataStore;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    com.slack.kaldb.proto.config.KaldbConfigs.ZookeeperConfig zkConfig =
        com.slack.kaldb.proto.config.KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("ReplicaRestoreServiceTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    KaldbConfigs.ManagerConfig.ReplicaRestoreServiceConfig replicaRecreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaRestoreServiceConfig.newBuilder()
            .addAllReplicaSets(List.of("rep1"))
            .setMaxReplicasPerRequest(200)
            .setReplicaLifespanMins(60)
            .setSchedulePeriodMins(30)
            .build();

    managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setReplicaRestoreServiceConfig(replicaRecreationServiceConfig)
            .build();

    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    replicaMetadataStore = spy(new ReplicaMetadataStore(curatorFramework, meterRegistry));
  }

  @AfterEach
  public void tearDown() throws IOException {
    meterRegistry.close();
    testingServer.close();
    curatorFramework.unwrap().close();
  }

  @Test
  public void shouldHandleDrainingAndAdding() throws Exception {
    doAnswer(
            invocationOnMock -> {
              Thread.sleep(100);
              return invocationOnMock.callRealMethod();
            })
        .when(replicaMetadataStore)
        .createSync(any(ReplicaMetadata.class));

    ReplicaRestoreService replicaRestoreService =
        new ReplicaRestoreService(replicaMetadataStore, meterRegistry, managerConfig);

    for (int i = 0; i < 10; i++) {
      long now = Instant.now().toEpochMilli();
      String id = "loop" + i;
      SnapshotMetadata snapshotIncluded =
          new SnapshotMetadata(id, id, now + 10, now + 15, 0, id, LOGS_LUCENE9);
      replicaRestoreService.queueSnapshotsForRestoration(List.of(snapshotIncluded));
      Thread.sleep(300);
    }

    await().until(() -> replicaMetadataStore.listSync().size() == 7);
    await()
        .until(
            () ->
                MetricsUtil.getTimerCount(
                    ReplicaRestoreService.REPLICAS_RESTORE_TIMER, meterRegistry),
            (value) -> value == 1);

    await().until(() -> replicaMetadataStore.listSync().size() == 10);
    await()
        .until(
            () ->
                MetricsUtil.getTimerCount(
                    ReplicaRestoreService.REPLICAS_RESTORE_TIMER, meterRegistry),
            (value) -> value == 2);
  }

  @Test
  public void shouldHandleMultipleSimultaneousRequests() {
    doAnswer(
            invocationOnMock -> {
              Thread.sleep(100);
              return invocationOnMock.callRealMethod();
            })
        .when(replicaMetadataStore)
        .createSync(any(ReplicaMetadata.class));

    ReplicaRestoreService replicaRestoreService =
        new ReplicaRestoreService(replicaMetadataStore, meterRegistry, managerConfig);
    ExecutorService executorService = Executors.newFixedThreadPool(2);

    for (int i = 0; i < 2; i++) {
      executorService.submit(
          () -> {
            for (int j = 0; j < 10; j++) {
              long now = Instant.now().toEpochMilli();
              String id = "loop" + UUID.randomUUID();
              SnapshotMetadata snapshotIncluded =
                  new SnapshotMetadata(id, id, now + 10, now + 15, 0, id, LOGS_LUCENE9);
              try {
                replicaRestoreService.queueSnapshotsForRestoration(List.of(snapshotIncluded));
                Thread.sleep(300);
              } catch (Exception e) {
                fail("Error in queueSnapshotsForRestoration", e);
              }
            }
          });
    }

    await().until(() -> replicaMetadataStore.listSync().size() == 14);
    await()
        .until(
            () ->
                MetricsUtil.getTimerCount(
                    ReplicaRestoreService.REPLICAS_RESTORE_TIMER, meterRegistry),
            (value) -> value == 1);
    await().until(() -> replicaMetadataStore.listSync().size() == 20);
    await()
        .until(
            () ->
                MetricsUtil.getTimerCount(
                    ReplicaRestoreService.REPLICAS_RESTORE_TIMER, meterRegistry),
            (value) -> value == 2);

    executorService.shutdown();
  }

  @Test
  public void shouldRemoveDuplicates() throws Exception {
    KaldbConfigs.ManagerConfig.ReplicaRestoreServiceConfig replicaRecreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaRestoreServiceConfig.newBuilder()
            .addAllReplicaSets(List.of("rep1"))
            .setMaxReplicasPerRequest(200)
            .setReplicaLifespanMins(60)
            .setSchedulePeriodMins(30)
            .build();

    managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaRestoreServiceConfig(replicaRecreationServiceConfig)
            .build();

    ReplicaRestoreService replicaRestoreService =
        new ReplicaRestoreService(replicaMetadataStore, meterRegistry, managerConfig);

    long now = Instant.now().toEpochMilli();
    List<SnapshotMetadata> duplicateSnapshots = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String id = "duplicate";
      duplicateSnapshots.add(new SnapshotMetadata(id, id, now + 10, now + 15, 0, id, LOGS_LUCENE9));
    }

    replicaRestoreService.queueSnapshotsForRestoration(duplicateSnapshots);

    await().until(() -> replicaMetadataStore.listSync().size() == 1);
    await()
        .until(
            () -> MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_SKIPPED, meterRegistry),
            (value) -> value == 9);
    await()
        .until(
            () -> MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_CREATED, meterRegistry),
            (value) -> value == 1);

    List<SnapshotMetadata> snapshots = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      now = Instant.now().toEpochMilli();
      String id = "loop" + i;
      snapshots.add(new SnapshotMetadata(id, id, now + 10, now + 15, 0, id, LOGS_LUCENE9));
    }

    replicaRestoreService.queueSnapshotsForRestoration(snapshots);
    replicaRestoreService.queueSnapshotsForRestoration(duplicateSnapshots);

    await().until(() -> replicaMetadataStore.listSync().size() == 4);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_SKIPPED, meterRegistry))
        .isEqualTo(19);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(4);
    assertThat(replicaMetadataStore.listSync().stream().filter(r -> r.isRestored).count())
        .isEqualTo(4);
  }

  @Test
  public void shouldNotQueueIfFull() {
    int MAX_QUEUE_SIZE = 5;
    KaldbConfigs.ManagerConfig.ReplicaRestoreServiceConfig replicaRecreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaRestoreServiceConfig.newBuilder()
            .setMaxReplicasPerRequest(MAX_QUEUE_SIZE)
            .setReplicaLifespanMins(60)
            .setSchedulePeriodMins(30)
            .build();

    managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaRestoreServiceConfig(replicaRecreationServiceConfig)
            .build();

    ReplicaRestoreService replicaRestoreService =
        new ReplicaRestoreService(replicaMetadataStore, meterRegistry, managerConfig);

    List<SnapshotMetadata> snapshots = new ArrayList<>();
    for (int i = 0; i < MAX_QUEUE_SIZE; i++) {
      long now = Instant.now().toEpochMilli();
      String id = "loop" + i;
      snapshots.add(new SnapshotMetadata(id, id, now + 10, now + 15, 0, id, LOGS_LUCENE9));
    }

    assertThatExceptionOfType(SizeLimitExceededException.class)
        .isThrownBy(() -> replicaRestoreService.queueSnapshotsForRestoration(snapshots));
  }
}
