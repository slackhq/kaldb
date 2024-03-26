package com.slack.kaldb.server;

import static com.slack.kaldb.metadata.snapshot.SnapshotMetadata.LIVE_SNAPSHOT_PATH;
import static com.slack.kaldb.proto.metadata.Metadata.IndexType.LOGS_LUCENE9;
import static com.slack.kaldb.server.RecoveryTaskCreator.RECOVERY_TASKS_CREATED;
import static com.slack.kaldb.server.RecoveryTaskCreator.STALE_SNAPSHOT_DELETE_SUCCESS;
import static com.slack.kaldb.server.RecoveryTaskCreator.getHighestDurableOffsetForPartition;
import static com.slack.kaldb.server.RecoveryTaskCreator.getStaleLiveSnapshots;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import brave.Tracing;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.core.InternalMetadataStoreException;
import com.slack.kaldb.metadata.core.KaldbMetadataTestUtils;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("UnstableApiUsage")
public class RecoveryTaskCreatorTest {
  private static final long TEST_MAX_MESSAGES_PER_RECOVERY_TASK = 10000;
  private SimpleMeterRegistry meterRegistry;
  private TestingServer testingServer;
  private AsyncCuratorFramework curatorFramework;
  private SnapshotMetadataStore snapshotMetadataStore;
  private RecoveryTaskMetadataStore recoveryTaskStore;
  private KaldbConfigs.IndexerConfig indexerConfig;
  private static final String partitionId = "1";

  @BeforeEach
  public void startup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("test")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(500)
            .build();

    // Default behavior
    this.indexerConfig =
        KaldbConfigs.IndexerConfig.newBuilder()
            .setMaxMessagesPerChunk(20)
            .setMaxBytesPerChunk(20)
            .setReadFromLocationOnStart(KaldbConfigs.KafkaOffsetLocation.EARLIEST)
            .setCreateRecoveryTasksOnStart(false)
            .build();

    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    snapshotMetadataStore = spy(new SnapshotMetadataStore(curatorFramework, meterRegistry));
    recoveryTaskStore = spy(new RecoveryTaskMetadataStore(curatorFramework, true, meterRegistry));
  }

  @AfterEach
  public void shutdown() throws IOException {
    if (recoveryTaskStore != null) {
      recoveryTaskStore.close();
    }
    if (snapshotMetadataStore != null) {
      snapshotMetadataStore.close();
    }
    if (curatorFramework != null) {
      curatorFramework.unwrap().close();
    }
    if (testingServer != null) {
      testingServer.close();
    }
    if (meterRegistry != null) {
      meterRegistry.close();
    }
  }

  @Test
  public void testStaleSnapshotDetection() {
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;

    SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId, LOGS_LUCENE9);
    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "1",
            LIVE_SNAPSHOT_PATH,
            startTime,
            endTime,
            maxOffset,
            partitionId,
            LOGS_LUCENE9);
    SnapshotMetadata livePartition11 =
        new SnapshotMetadata(
            name + "11",
            LIVE_SNAPSHOT_PATH,
            startTime,
            endTime,
            maxOffset,
            partitionId,
            LOGS_LUCENE9);
    SnapshotMetadata livePartition2 =
        new SnapshotMetadata(
            name + "2", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, "2", LOGS_LUCENE9);
    SnapshotMetadata partition2 =
        new SnapshotMetadata(name + "3", path, startTime, endTime, maxOffset, "2", LOGS_LUCENE9);

    assertThat(getStaleLiveSnapshots(List.of(partition1), partitionId)).isEmpty();
    assertThat(getStaleLiveSnapshots(List.of(partition2), partitionId)).isEmpty();
    assertThat(getStaleLiveSnapshots(List.of(livePartition2), partitionId)).isEmpty();
    assertThat(getStaleLiveSnapshots(List.of(livePartition1), partitionId))
        .containsExactly(livePartition1);
    assertThat(getStaleLiveSnapshots(List.of(livePartition1, livePartition11), partitionId))
        .containsExactly(livePartition1, livePartition11);
    assertThat(getStaleLiveSnapshots(List.of(partition1, livePartition1), partitionId))
        .containsExactly(livePartition1);
    assertThat(getStaleLiveSnapshots(List.of(partition2, livePartition1), partitionId))
        .containsExactly(livePartition1);
    assertThat(getStaleLiveSnapshots(List.of(livePartition2, livePartition1), partitionId))
        .containsExactly(livePartition1);
    assertThat(
            getStaleLiveSnapshots(
                List.of(livePartition2, livePartition1, partition1, partition2), partitionId))
        .containsExactly(livePartition1);
    assertThat(
            getStaleLiveSnapshots(
                List.of(livePartition2, livePartition1, livePartition11, partition1, partition2),
                partitionId))
        .containsExactly(livePartition1, livePartition11);
    assertThat(getStaleLiveSnapshots(List.of(partition1, partition2), partitionId)).isEmpty();
  }

  @Test
  public void testDeleteStaleSnapshotDeletion() {
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;

    SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId, LOGS_LUCENE9);
    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "1",
            LIVE_SNAPSHOT_PATH,
            startTime,
            endTime,
            maxOffset,
            partitionId,
            LOGS_LUCENE9);
    SnapshotMetadata livePartition11 =
        new SnapshotMetadata(
            name + "11",
            LIVE_SNAPSHOT_PATH,
            startTime,
            endTime,
            maxOffset,
            partitionId,
            LOGS_LUCENE9);
    SnapshotMetadata livePartition2 =
        new SnapshotMetadata(
            name + "2", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, "2", LOGS_LUCENE9);
    SnapshotMetadata partition2 =
        new SnapshotMetadata(name + "3", path, startTime, endTime, maxOffset, "2", LOGS_LUCENE9);

    testDeleteSnapshots(List.of(partition1), 0, List.of(partition1));
    testDeleteSnapshots(List.of(partition2), 0, List.of(partition2));
    testDeleteSnapshots(List.of(livePartition2), 0, List.of(livePartition2));
    testDeleteSnapshots(List.of(livePartition1), 1, Collections.emptyList());
    testDeleteSnapshots(List.of(livePartition1, livePartition11), 2, Collections.emptyList());
    testDeleteSnapshots(List.of(partition1, livePartition1), 1, List.of(partition1));
    testDeleteSnapshots(
        List.of(partition1, livePartition1, livePartition11), 2, List.of(partition1));
    testDeleteSnapshots(
        List.of(partition2, livePartition2), 0, List.of(partition2, livePartition2));
    testDeleteSnapshots(List.of(partition2, partition1), 0, List.of(partition2, partition1));
    testDeleteSnapshots(
        List.of(partition2, partition1, livePartition11), 1, List.of(partition2, partition1));
    testDeleteSnapshots(
        List.of(partition1, livePartition1, partition2, livePartition2),
        1,
        List.of(partition2, partition1, livePartition2));
    testDeleteSnapshots(
        List.of(partition1, livePartition1, livePartition11, partition2, livePartition2),
        2,
        List.of(partition2, partition1, livePartition2));
  }

  private void testDeleteSnapshots(
      List<SnapshotMetadata> actualSnapshots,
      int deletedSnapshotSize,
      List<SnapshotMetadata> expectedSnapshots) {
    actualSnapshots.forEach(snapshot -> snapshotMetadataStore.createSync(snapshot));
    await().until(() -> snapshotMetadataStore.listSync().containsAll(actualSnapshots));

    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            1,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);
    assertThat(recoveryTaskCreator.deleteStaleLiveSnapshots(actualSnapshots).size())
        .isEqualTo(deletedSnapshotSize);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsExactlyInAnyOrderElementsOf(expectedSnapshots);
    // Clear state
    expectedSnapshots.forEach(snapshot -> snapshotMetadataStore.deleteSync(snapshot));
  }

  @Test
  public void shouldStaleDeletionShouldHandleTimeouts() {
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;

    SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId, LOGS_LUCENE9);
    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "1",
            LIVE_SNAPSHOT_PATH,
            startTime,
            endTime,
            maxOffset,
            partitionId,
            LOGS_LUCENE9);
    SnapshotMetadata livePartition11 =
        new SnapshotMetadata(
            name + "11",
            LIVE_SNAPSHOT_PATH,
            startTime,
            endTime,
            maxOffset,
            partitionId,
            LOGS_LUCENE9);
    SnapshotMetadata livePartition2 =
        new SnapshotMetadata(
            name + "2", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, "2", LOGS_LUCENE9);
    SnapshotMetadata partition2 =
        new SnapshotMetadata(name + "3", path, startTime, endTime, maxOffset, "2", LOGS_LUCENE9);

    testDeleteSnapshotsTimeouts(List.of(partition1), List.of(partition1), false);
    testDeleteSnapshotsTimeouts(List.of(livePartition1), List.of(livePartition1), true);
    testDeleteSnapshotsTimeouts(
        List.of(partition1, livePartition1), List.of(partition1, livePartition1), true);
    testDeleteSnapshotsTimeouts(
        List.of(partition1, livePartition1, livePartition11),
        List.of(partition1, livePartition1, livePartition11),
        true);
    testDeleteSnapshotsTimeouts(
        List.of(partition1, livePartition1, livePartition11, partition2),
        List.of(partition1, livePartition1, livePartition11, partition2),
        true);
    testDeleteSnapshotsTimeouts(
        List.of(partition1, livePartition1, livePartition11, partition2, livePartition2),
        List.of(partition1, livePartition1, livePartition11, partition2, livePartition2),
        true);
  }

  private void testDeleteSnapshotsTimeouts(
      List<SnapshotMetadata> actualSnapshots,
      List<SnapshotMetadata> expectedSnapshots,
      boolean hasException) {

    actualSnapshots.forEach(snapshot -> snapshotMetadataStore.createSync(snapshot));
    await().until(() -> snapshotMetadataStore.listSync().containsAll(actualSnapshots));

    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            1,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    // Throw exceptions on delete.
    AsyncStage asyncStage = mock(AsyncStage.class);
    when(asyncStage.toCompletableFuture())
        .thenReturn(CompletableFuture.failedFuture(new Exception()));

    doReturn(asyncStage).when(snapshotMetadataStore).deleteAsync(any(SnapshotMetadata.class));

    if (hasException) {
      assertThatIllegalStateException()
          .isThrownBy(() -> recoveryTaskCreator.deleteStaleLiveSnapshots(actualSnapshots));
    } else {
      assertThat(recoveryTaskCreator.deleteStaleLiveSnapshots(actualSnapshots)).isEmpty();
    }

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsExactlyInAnyOrderElementsOf(expectedSnapshots);

    // Clear state but reset the overloaded method.
    doCallRealMethod().when(snapshotMetadataStore).deleteAsync((SnapshotMetadata) any());
    expectedSnapshots.forEach(snapshot -> snapshotMetadataStore.deleteSync(snapshot));
  }

  @Test
  public void shouldStaleDeletionShouldHandleExceptions() {
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId, LOGS_LUCENE9);
    final SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "1",
            LIVE_SNAPSHOT_PATH,
            startTime,
            endTime,
            maxOffset,
            partitionId,
            LOGS_LUCENE9);
    final SnapshotMetadata livePartition11 =
        new SnapshotMetadata(
            name + "11",
            LIVE_SNAPSHOT_PATH,
            startTime,
            endTime,
            maxOffset,
            partitionId,
            LOGS_LUCENE9);
    final SnapshotMetadata livePartition2 =
        new SnapshotMetadata(
            name + "2", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset, "2", LOGS_LUCENE9);
    final SnapshotMetadata partition2 =
        new SnapshotMetadata(name + "3", path, startTime, endTime, maxOffset, "2", LOGS_LUCENE9);

    List<SnapshotMetadata> snapshots =
        List.of(partition1, livePartition1, livePartition11, partition2, livePartition2);

    snapshots.forEach(snapshot -> snapshotMetadataStore.createSync(snapshot));
    await().until(() -> snapshotMetadataStore.listSync().containsAll(snapshots));

    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            1,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    // Pass first call, fail second call.
    ExecutorService timeoutServiceExecutor = Executors.newSingleThreadExecutor();

    AsyncStage asyncStage1 = mock(AsyncStage.class);
    when(asyncStage1.toCompletableFuture())
        .thenReturn(
            CompletableFuture.runAsync(
                () -> {
                  try {
                    Thread.sleep(30 * 1000);
                  } catch (InterruptedException ignored) {
                  }
                },
                timeoutServiceExecutor));

    // allow the first deletion to work, and timeout the second one
    doCallRealMethod()
        .doReturn(asyncStage1)
        .when(snapshotMetadataStore)
        .deleteAsync(any(SnapshotMetadata.class));

    AsyncStage asyncStage2 = mock(AsyncStage.class);
    when(asyncStage2.toCompletableFuture())
        .thenReturn(CompletableFuture.failedFuture(new Exception()));

    doCallRealMethod()
        .doReturn(asyncStage2)
        .when(snapshotMetadataStore)
        .deleteAsync((SnapshotMetadata) any());

    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.deleteStaleLiveSnapshots(snapshots));

    // Either liveSnapshot1 or liveSnapshot11 remain but not both.
    List<SnapshotMetadata> actualSnapshots =
        KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(actualSnapshots.size()).isEqualTo(4);
    assertThat(actualSnapshots).contains(partition1, partition2, livePartition2);
    assertThat(
            (actualSnapshots.contains(livePartition1) && !actualSnapshots.contains(livePartition11))
                || (actualSnapshots.contains(livePartition11)
                    && !actualSnapshots.contains(livePartition1)))
        .isTrue();
  }

  @Test
  public void testMaxOffset() {
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId, LOGS_LUCENE9);
    final SnapshotMetadata partition11 =
        new SnapshotMetadata(
            name + "1", path, endTime + 1, endTime * 2, maxOffset * 2, partitionId, LOGS_LUCENE9);
    final SnapshotMetadata partition12 =
        new SnapshotMetadata(
            name + "12",
            path,
            endTime * 2 + 1,
            endTime * 3,
            maxOffset * 3,
            partitionId,
            LOGS_LUCENE9);

    final String partitionId2 = "2";
    final long partition2Offset = maxOffset * 10;
    final SnapshotMetadata partition2 =
        new SnapshotMetadata(
            name + "2", path, startTime, endTime, partition2Offset, partitionId2, LOGS_LUCENE9);
    final SnapshotMetadata partition21 =
        new SnapshotMetadata(
            name + "21",
            path,
            endTime + 1,
            endTime * 2,
            partition2Offset * 2,
            partitionId2,
            LOGS_LUCENE9);
    final SnapshotMetadata partition22 =
        new SnapshotMetadata(
            name + "22",
            path,
            endTime * 2 + 1,
            endTime * 3,
            partition2Offset * 3,
            partitionId2,
            LOGS_LUCENE9);

    // empty results
    assertThat(
            getHighestDurableOffsetForPartition(
                Collections.emptyList(), Collections.emptyList(), partitionId))
        .isNegative();

    // Some snapshots, no recovery tasks.
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1), Collections.emptyList(), partitionId))
        .isEqualTo(maxOffset);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition2), Collections.emptyList(), partitionId))
        .isEqualTo(maxOffset);

    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition11), Collections.emptyList(), partitionId))
        .isEqualTo(maxOffset * 2);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition11, partition12),
                Collections.emptyList(),
                partitionId))
        .isEqualTo(maxOffset * 3);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition2, partition21), Collections.emptyList(), partitionId))
        .isEqualTo(maxOffset);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition12, partition2, partition21, partition22),
                Collections.emptyList(),
                partitionId))
        .isEqualTo(maxOffset * 3);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition2), Collections.emptyList(), partitionId))
        .isNegative();
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition2, partition21, partition22),
                Collections.emptyList(),
                partitionId))
        .isNegative();
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(
                    partition1,
                    partition11,
                    partition12,
                    partition21,
                    partition2,
                    partition21,
                    partition22),
                Collections.emptyList(),
                partitionId))
        .isEqualTo(maxOffset * 3);

    // Only recovery tasks, no snapshots.
    final String recoveryTaskName = "recoveryTask";
    final long recoveryStartOffset = 400;
    final long createdTimeUtc = Instant.now().toEpochMilli();

    final RecoveryTaskMetadata recoveryTask1 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "1",
            partitionId,
            recoveryStartOffset,
            recoveryStartOffset * 2,
            createdTimeUtc);
    final RecoveryTaskMetadata recoveryTask11 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "11",
            partitionId,
            recoveryStartOffset * 2 + 1,
            recoveryStartOffset * 3,
            createdTimeUtc);
    final RecoveryTaskMetadata recoveryTask21 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "21",
            partitionId2,
            recoveryStartOffset * 5 + 1,
            recoveryStartOffset * 6,
            createdTimeUtc);
    final RecoveryTaskMetadata recoveryTask22 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "21",
            partitionId2,
            recoveryStartOffset * 6 + 1,
            recoveryStartOffset * 7,
            createdTimeUtc);

    assertThat(
            getHighestDurableOffsetForPartition(
                Collections.emptyList(), List.of(recoveryTask1), partitionId))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            getHighestDurableOffsetForPartition(
                Collections.emptyList(), List.of(recoveryTask1, recoveryTask22), partitionId))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            getHighestDurableOffsetForPartition(
                Collections.emptyList(), List.of(recoveryTask1, recoveryTask11), partitionId))
        .isEqualTo(recoveryStartOffset * 3);
    assertThat(
            getHighestDurableOffsetForPartition(
                Collections.emptyList(), List.of(recoveryTask11, recoveryTask21), partitionId))
        .isEqualTo(recoveryStartOffset * 3);
    assertThat(
            getHighestDurableOffsetForPartition(
                Collections.emptyList(),
                List.of(recoveryTask1, recoveryTask11, recoveryTask21),
                partitionId))
        .isEqualTo(recoveryStartOffset * 3);
    assertThat(
            getHighestDurableOffsetForPartition(
                Collections.emptyList(),
                List.of(recoveryTask1, recoveryTask11, recoveryTask21, recoveryTask22),
                partitionId))
        .isEqualTo(recoveryStartOffset * 3);

    //  snapshots and recovery tasks for same partition
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1), List.of(recoveryTask1), partitionId))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition11), List.of(recoveryTask1), partitionId))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition11, partition12), List.of(recoveryTask1), partitionId))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition11, partition12),
                List.of(recoveryTask1, recoveryTask11),
                partitionId))
        .isEqualTo(recoveryStartOffset * 3);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition2), List.of(recoveryTask21), partitionId))
        .isNegative();
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition2, partition21),
                List.of(recoveryTask21, recoveryTask22),
                partitionId))
        .isNegative();

    //  snapshots for diff partitions, recovery tasks for diff partitions.
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition2, partition12), List.of(recoveryTask1), partitionId))
        .isEqualTo(recoveryStartOffset * 2);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition2, partition12), List.of(recoveryTask11), partitionId))
        .isEqualTo(recoveryStartOffset * 3);
    assertThat(
            getHighestDurableOffsetForPartition(
                List.of(partition1, partition11, partition12, partition2, partition21, partition22),
                List.of(recoveryTask1, recoveryTask11, recoveryTask21, recoveryTask22),
                partitionId))
        .isEqualTo(recoveryStartOffset * 3);
  }

  @Test
  public void testInit() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryTaskCreator(
                    snapshotMetadataStore,
                    recoveryTaskStore,
                    partitionId,
                    0,
                    TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
                    meterRegistry));

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryTaskCreator(
                    snapshotMetadataStore,
                    recoveryTaskStore,
                    "",
                    100,
                    TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
                    meterRegistry));

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryTaskCreator(
                    snapshotMetadataStore, recoveryTaskStore, partitionId, 100, 0, meterRegistry));

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new RecoveryTaskCreator(
                    snapshotMetadataStore, recoveryTaskStore, partitionId, 100, -1, meterRegistry));
  }

  @Test
  public void
      testDetermineStartingOffsetReturnsHeadAndCreatesRecoveryTasksWhenCreateTasksIsTrueAndOffsetLocationIsHead() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();

    KaldbConfigs.IndexerConfig headLocationAndRecoveryConfig =
        KaldbConfigs.IndexerConfig.newBuilder()
            .setCreateRecoveryTasksOnStart(true)
            .setReadFromLocationOnStart(KaldbConfigs.KafkaOffsetLocation.LATEST)
            .setMaxMessagesPerChunk(100)
            .build();

    assertThat(recoveryTaskCreator.determineStartingOffset(100, 10, headLocationAndRecoveryConfig))
        .isEqualTo(100);
    List<RecoveryTaskMetadata> recoveryTasks =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore);
    assertThat(recoveryTasks.size()).isEqualTo(1);
    assertThat(recoveryTasks.get(0).startOffset).isEqualTo(10);
    assertThat(recoveryTasks.get(0).endOffset).isEqualTo(100);
    assertThat(recoveryTasks.get(0).partitionId).isEqualTo(partitionId);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(1);
  }

  @Test
  public void
      testDetermineStartingOffsetReturnsHeadWhenCreateTasksIsFalseAndOffsetLocationIsHead() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            1,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();

    KaldbConfigs.IndexerConfig headLocationAndNoRecoveryConfig =
        KaldbConfigs.IndexerConfig.newBuilder()
            .setCreateRecoveryTasksOnStart(false)
            .setReadFromLocationOnStart(KaldbConfigs.KafkaOffsetLocation.LATEST)
            .build();

    // When there is no data and ReadFromLocationOnStart is set to LATEST, return the current head
    assertThat(
            recoveryTaskCreator.determineStartingOffset(1000, 0, headLocationAndNoRecoveryConfig))
        .isEqualTo(1000);

    // Data exists for not for this partition.
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, "2", LOGS_LUCENE9);
    snapshotMetadataStore.createSync(partition1);
    await().until(() -> snapshotMetadataStore.listSync().contains(partition1));

    assertThat(recoveryTaskCreator.determineStartingOffset(1, -1, headLocationAndNoRecoveryConfig))
        .isEqualTo(1);

    final SnapshotMetadata partition11 =
        new SnapshotMetadata(
            name + "1", path, endTime + 1, endTime * 2, maxOffset * 2, "2", LOGS_LUCENE9);
    snapshotMetadataStore.createSync(partition11);
    await().until(() -> snapshotMetadataStore.listSync().contains(partition11));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .contains(partition1, partition11);
    assertThat(recoveryTaskCreator.determineStartingOffset(0, 0, indexerConfig)).isNegative();

    final String recoveryTaskName = "recoveryTask";
    final long recoveryStartOffset = 400;
    final long createdTimeUtc = Instant.now().toEpochMilli();

    final RecoveryTaskMetadata recoveryTask1 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "1",
            "2",
            recoveryStartOffset,
            recoveryStartOffset * 2,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask1);
    await().until(() -> recoveryTaskStore.listSync().contains(recoveryTask1));
    assertThat(recoveryTaskCreator.determineStartingOffset(1, -1, headLocationAndNoRecoveryConfig))
        .isEqualTo(1);
  }

  @Test
  public void testDetermineStartOffsetReturnsNegativeWhenNoOffset() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            1,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();

    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000, 0, indexerConfig)).isNegative();
    assertThat(recoveryTaskCreator.determineStartingOffset(0, 0, indexerConfig)).isNegative();

    // Data exists for not for this partition.
    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, "2", LOGS_LUCENE9);
    snapshotMetadataStore.createSync(partition1);
    await().until(() -> snapshotMetadataStore.listSync().contains(partition1));
    assertThat(recoveryTaskCreator.determineStartingOffset(0, 0, indexerConfig)).isNegative();

    final SnapshotMetadata partition11 =
        new SnapshotMetadata(
            name + "1", path, endTime + 1, endTime * 2, maxOffset * 2, "2", LOGS_LUCENE9);
    snapshotMetadataStore.createSync(partition11);
    await().until(() -> snapshotMetadataStore.listSync().contains(partition11));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .contains(partition1, partition11);
    assertThat(recoveryTaskCreator.determineStartingOffset(0, 0, indexerConfig)).isNegative();

    final String recoveryTaskName = "recoveryTask";
    final long recoveryStartOffset = 400;
    final long createdTimeUtc = Instant.now().toEpochMilli();

    final RecoveryTaskMetadata recoveryTask1 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "1",
            "2",
            recoveryStartOffset,
            recoveryStartOffset * 2,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask1);
    await().until(() -> recoveryTaskStore.listSync().contains(recoveryTask1));
    assertThat(recoveryTaskCreator.determineStartingOffset(0, 0, indexerConfig)).isNegative();
  }

  @Test
  public void testDetermineStartingOffsetOnlyRecoveryNotBehind() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();

    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000, 0, indexerConfig)).isNegative();
    final String recoveryTaskName = "recoveryTask";
    final long recoveryStartOffset = 400;
    final long createdTimeUtc = Instant.now().toEpochMilli();

    final RecoveryTaskMetadata recoveryTask1 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "1",
            partitionId,
            recoveryStartOffset,
            recoveryStartOffset * 2,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask1);
    await().until(() -> recoveryTaskStore.listSync().contains(recoveryTask1));
    assertThat(recoveryTaskCreator.determineStartingOffset(850, 0, indexerConfig))
        .isEqualTo((recoveryStartOffset * 2) + 1);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(750, 0, indexerConfig));

    final RecoveryTaskMetadata recoveryTask11 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "11",
            partitionId,
            recoveryStartOffset * 2 + 1,
            recoveryStartOffset * 3,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask11);
    await()
        .until(
            () -> recoveryTaskStore.listSync().containsAll(List.of(recoveryTask1, recoveryTask11)));
    assertThat(recoveryTaskCreator.determineStartingOffset(1201, 0, indexerConfig))
        .isEqualTo((recoveryStartOffset * 3) + 1);
    assertThat(recoveryTaskCreator.determineStartingOffset(1200, 0, indexerConfig))
        .isEqualTo((recoveryStartOffset * 3) + 1);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(1150, 0, indexerConfig));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore))
        .contains(recoveryTask1, recoveryTask11);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
  }

  @Test
  public void testDetermineStartingOffsetOnlyRecoveryBehind() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();

    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000, 0, indexerConfig)).isNegative();
    final String recoveryTaskName = "recoveryTask";
    final long recoveryStartOffset = 400;
    final long createdTimeUtc = Instant.now().toEpochMilli();

    final RecoveryTaskMetadata recoveryTask1 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "1",
            partitionId,
            recoveryStartOffset,
            recoveryStartOffset * 2,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask1);
    await().until(() -> recoveryTaskStore.listSync().contains(recoveryTask1));
    final long currentHeadOffset = 4000;
    assertThat(recoveryTaskCreator.determineStartingOffset(currentHeadOffset, 0, indexerConfig))
        .isEqualTo(currentHeadOffset);
    List<RecoveryTaskMetadata> recoveryTasks =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore);
    assertThat(recoveryTasks.size()).isEqualTo(2);
    assertThat(recoveryTasks).contains(recoveryTask1);
    Optional<RecoveryTaskMetadata> newRecoveryTask =
        recoveryTasks.stream().filter(r -> !r.equals(recoveryTask1)).findFirst();
    assertThat(newRecoveryTask).isNotEmpty();
    RecoveryTaskMetadata recoveryTask = newRecoveryTask.get();
    assertThat(recoveryTask.startOffset).isEqualTo(recoveryStartOffset * 2 + 1);
    assertThat(recoveryTask.endOffset).isEqualTo(currentHeadOffset - 1);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
  }

  @Test
  public void testDetermineStartingOffsetOnlyMultipleRecoveryBehind() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();

    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000, 0, indexerConfig)).isNegative();
    final String recoveryTaskName = "BasicRecoveryTask";
    final long recoveryStartOffset = 400;
    final long createdTimeUtc = Instant.now().toEpochMilli();

    final RecoveryTaskMetadata recoveryTask1 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "1",
            partitionId,
            recoveryStartOffset,
            recoveryStartOffset * 2,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask1);
    final RecoveryTaskMetadata recoveryTask11 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "11",
            partitionId,
            recoveryStartOffset * 2 + 1,
            recoveryStartOffset * 3,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask11);
    await()
        .until(
            () -> recoveryTaskStore.listSync().containsAll(List.of(recoveryTask1, recoveryTask11)));

    final long currentHeadOffset = 4000;
    assertThat(recoveryTaskCreator.determineStartingOffset(currentHeadOffset, 0, indexerConfig))
        .isEqualTo(currentHeadOffset);
    List<RecoveryTaskMetadata> recoveryTasks =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore);
    assertThat(recoveryTasks.size()).isEqualTo(3);
    assertThat(recoveryTasks).contains(recoveryTask1, recoveryTask11);
    Optional<RecoveryTaskMetadata> newRecoveryTask =
        recoveryTasks.stream().filter(r -> !r.name.contains(recoveryTaskName)).findFirst();
    assertThat(newRecoveryTask).isNotEmpty();
    RecoveryTaskMetadata recoveryTask = newRecoveryTask.get();
    assertThat(recoveryTask.startOffset).isEqualTo(recoveryStartOffset * 3 + 1);
    assertThat(recoveryTask.endOffset).isEqualTo(currentHeadOffset - 1);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
  }

  @Test
  public void testDetermineStartingOffsetMultiplePartitionRecoveriesBehind() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();

    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000, 0, indexerConfig)).isNegative();
    final String recoveryTaskName = "BasicRecoveryTask";
    final long recoveryStartOffset = 400;
    final long createdTimeUtc = Instant.now().toEpochMilli();

    final RecoveryTaskMetadata recoveryTask1 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "1",
            partitionId,
            recoveryStartOffset,
            recoveryStartOffset * 2,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask1);
    final RecoveryTaskMetadata recoveryTask11 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "11",
            partitionId,
            recoveryStartOffset * 2 + 1,
            recoveryStartOffset * 3,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask11);
    final RecoveryTaskMetadata recoveryTask2 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "2",
            "2",
            recoveryStartOffset * 3 + 1,
            recoveryStartOffset * 4,
            createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask2);
    final RecoveryTaskMetadata recoveryTask21 =
        new RecoveryTaskMetadata(
            recoveryTaskName + "21", "2", recoveryStartOffset * 4 + 1, 50000, createdTimeUtc);
    recoveryTaskStore.createSync(recoveryTask21);
    await()
        .until(
            () ->
                recoveryTaskStore
                    .listSync()
                    .containsAll(
                        List.of(recoveryTask1, recoveryTask11, recoveryTask2, recoveryTask21)));

    final long currentHeadOffset = 4000;
    assertThat(recoveryTaskCreator.determineStartingOffset(currentHeadOffset, 0, indexerConfig))
        .isEqualTo(currentHeadOffset);
    List<RecoveryTaskMetadata> recoveryTasks =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore);
    assertThat(recoveryTasks.size()).isEqualTo(5);
    assertThat(recoveryTasks)
        .contains(recoveryTask1, recoveryTask11, recoveryTask2, recoveryTask21);
    Optional<RecoveryTaskMetadata> newRecoveryTask =
        recoveryTasks.stream().filter(r -> !r.name.contains(recoveryTaskName)).findFirst();
    assertThat(newRecoveryTask).isNotEmpty();
    RecoveryTaskMetadata recoveryTask = newRecoveryTask.get();
    assertThat(recoveryTask.startOffset).isEqualTo((recoveryStartOffset * 3) + 1);
    assertThat(recoveryTask.endOffset).isEqualTo(currentHeadOffset - 1);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
  }

  @Test
  public void testDetermineStartingOffsetOnlySnapshotsNoDelay() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000, 0, indexerConfig)).isNegative();

    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId, LOGS_LUCENE9);
    snapshotMetadataStore.createSync(partition1);
    await().until(() -> snapshotMetadataStore.listSync().contains(partition1));
    assertThat(
            getHighestDurableOffsetForPartition(
                KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore),
                Collections.emptyList(),
                partitionId))
        .isEqualTo(100);
    assertThat(recoveryTaskCreator.determineStartingOffset(150, 0, indexerConfig)).isEqualTo(101);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(50, 0, indexerConfig));

    final SnapshotMetadata partition11 =
        new SnapshotMetadata(
            name + "11", path, endTime + 1, endTime * 2, maxOffset * 2, partitionId, LOGS_LUCENE9);

    snapshotMetadataStore.createSync(partition11);
    await().until(() -> snapshotMetadataStore.listSync().contains(partition11));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsExactlyInAnyOrder(partition1, partition11);
    assertThat(recoveryTaskCreator.determineStartingOffset(250, 0, indexerConfig)).isEqualTo(201);
    assertThat(recoveryTaskCreator.determineStartingOffset(201, 0, indexerConfig)).isEqualTo(201);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(150, 0, indexerConfig));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(0);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(0);

    // Live partition is cleaned up, no delay.
    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "live1",
            LIVE_SNAPSHOT_PATH,
            startTime,
            endTime,
            maxOffset,
            partitionId,
            LOGS_LUCENE9);
    snapshotMetadataStore.createSync(livePartition1);
    await().until(() -> snapshotMetadataStore.listSync().contains(livePartition1));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .contains(partition1, partition11, livePartition1);
    assertThat(recoveryTaskCreator.determineStartingOffset(250, 0, indexerConfig)).isEqualTo(201);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsExactlyInAnyOrder(partition1, partition11);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(1);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(0);

    // Multiple live partitions for the same partition are cleaned up, no delay.
    snapshotMetadataStore.createSync(livePartition1);
    SnapshotMetadata livePartition11 =
        new SnapshotMetadata(
            name + "live11",
            LIVE_SNAPSHOT_PATH,
            startTime,
            endTime,
            maxOffset,
            partitionId,
            LOGS_LUCENE9);
    snapshotMetadataStore.createSync(livePartition11);
    await().until(() -> snapshotMetadataStore.listSync().contains(livePartition11));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .contains(partition1, partition11, livePartition1, livePartition11);
    assertThat(recoveryTaskCreator.determineStartingOffset(250, 0, indexerConfig)).isEqualTo(201);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsExactlyInAnyOrder(partition1, partition11);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(3);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(0);

    // Live partitions from multiple stores exist.
    snapshotMetadataStore.createSync(livePartition1);
    snapshotMetadataStore.createSync(livePartition11);
    SnapshotMetadata livePartition2 =
        new SnapshotMetadata(
            name + "2", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset * 5, "2", LOGS_LUCENE9);
    snapshotMetadataStore.createSync(livePartition2);
    await()
        .until(
            () ->
                snapshotMetadataStore
                    .listSync()
                    .containsAll(List.of(livePartition1, livePartition11, livePartition2)));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .contains(partition1, partition11, livePartition1, livePartition2);
    assertThat(recoveryTaskCreator.determineStartingOffset(250, 0, indexerConfig)).isEqualTo(201);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsExactlyInAnyOrder(partition1, partition11, livePartition2);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(5);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(0);

    // Live and non-live partitions for different partitions exist.
    snapshotMetadataStore.createSync(livePartition1);
    snapshotMetadataStore.createSync(livePartition11);
    SnapshotMetadata partition2 =
        new SnapshotMetadata(
            name + "3", path, startTime, endTime, maxOffset * 3, "2", LOGS_LUCENE9);
    snapshotMetadataStore.createSync(partition2);
    await()
        .until(
            () ->
                snapshotMetadataStore
                    .listSync()
                    .containsAll(List.of(livePartition1, livePartition11, partition2)));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .contains(partition1, partition11, livePartition1, livePartition2, partition2);
    assertThat(recoveryTaskCreator.determineStartingOffset(250, 0, indexerConfig)).isEqualTo(201);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsExactlyInAnyOrder(partition1, partition11, livePartition2, partition2);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(7);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(0);
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void testDetermineStartingOffsetOnlySnapshotsWithDelay() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000, 0, indexerConfig)).isNegative();

    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId, LOGS_LUCENE9);
    snapshotMetadataStore.createSync(partition1);
    await().until(() -> snapshotMetadataStore.listSync().contains(partition1));
    assertThat(
            getHighestDurableOffsetForPartition(
                KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore),
                Collections.emptyList(),
                partitionId))
        .isEqualTo(100);
    assertThat(recoveryTaskCreator.determineStartingOffset(1150, 0, indexerConfig)).isEqualTo(1150);
    List<RecoveryTaskMetadata> recoveryTasks1 =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore);
    assertThat(recoveryTasks1.size()).isEqualTo(1);
    assertThat(recoveryTasks1.get(0).startOffset).isEqualTo(101);
    assertThat(recoveryTasks1.get(0).endOffset).isEqualTo(1149);
    assertThat(recoveryTasks1.get(0).partitionId).isEqualTo(partitionId);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(50, 0, indexerConfig));
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(0);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(1);
    // clean up recovery task.
    recoveryTaskStore.deleteSync(recoveryTasks1.get(0).name);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();

    final SnapshotMetadata partition11 =
        new SnapshotMetadata(
            name + "11", path, endTime + 1, endTime * 2, maxOffset * 2, partitionId, LOGS_LUCENE9);

    snapshotMetadataStore.createSync(partition11);
    await().until(() -> snapshotMetadataStore.listSync().contains(partition11));
    assertThat(recoveryTaskCreator.determineStartingOffset(1250, 0, indexerConfig)).isEqualTo(1250);

    AtomicReference<List<RecoveryTaskMetadata>> recoveryTasks = new AtomicReference<>();
    await()
        .until(
            () -> {
              recoveryTasks.set(recoveryTaskStore.listSync());
              return recoveryTasks.get().size() == 1;
            });
    RecoveryTaskMetadata recoveryTask1 = recoveryTasks.get().get(0);
    assertThat(recoveryTask1.startOffset).isEqualTo(201);
    assertThat(recoveryTask1.endOffset).isEqualTo(1249);
    assertThat(recoveryTask1.partitionId).isEqualTo(partitionId);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(1248, 0, indexerConfig));
    assertThat(recoveryTaskCreator.determineStartingOffset(1249, 0, indexerConfig)).isEqualTo(1250);
    assertThat(recoveryTaskCreator.determineStartingOffset(1250, 0, indexerConfig)).isEqualTo(1250);
    assertThat(recoveryTaskCreator.determineStartingOffset(1251, 0, indexerConfig)).isEqualTo(1250);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore).size()).isEqualTo(1);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(0);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(2);

    // Live partition is cleaned up, new recovery task is created.
    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "live1",
            LIVE_SNAPSHOT_PATH,
            startTime,
            endTime,
            maxOffset,
            partitionId,
            LOGS_LUCENE9);
    snapshotMetadataStore.createSync(livePartition1);
    await()
        .until(
            () ->
                snapshotMetadataStore
                    .listSync()
                    .containsAll(List.of(partition1, partition11, livePartition1)));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore))
        .containsExactly(recoveryTask1);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .contains(partition1, partition11, livePartition1);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(250, 0, indexerConfig));
    assertThat(recoveryTaskCreator.determineStartingOffset(1450, 0, indexerConfig)).isEqualTo(1450);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsExactlyInAnyOrder(partition1, partition11);
    List<RecoveryTaskMetadata> recoveryTasks2 =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore);
    assertThat(recoveryTasks2.size()).isEqualTo(2);
    RecoveryTaskMetadata recoveryTask2 =
        recoveryTasks2.stream().filter(r -> !recoveryTask1.equals(r)).findFirst().get();
    assertThat(recoveryTask2.startOffset).isEqualTo(1250);
    assertThat(recoveryTask2.endOffset).isEqualTo(1449);
    assertThat(recoveryTask2.partitionId).isEqualTo(partitionId);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore))
        .containsExactlyInAnyOrder(recoveryTask1, recoveryTask2);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(1);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(3);

    // Multiple live partitions for the same partition are cleaned up.
    snapshotMetadataStore.createSync(livePartition1);
    SnapshotMetadata livePartition11 =
        new SnapshotMetadata(
            name + "live11",
            LIVE_SNAPSHOT_PATH,
            startTime,
            endTime,
            maxOffset,
            partitionId,
            LOGS_LUCENE9);
    snapshotMetadataStore.createSync(livePartition11);
    await()
        .until(
            () ->
                snapshotMetadataStore
                    .listSync()
                    .containsAll(List.of(livePartition1, livePartition11)));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .contains(partition1, partition11, livePartition1, livePartition11);
    assertThat(recoveryTaskCreator.determineStartingOffset(1500, 0, indexerConfig)).isEqualTo(1450);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsExactlyInAnyOrder(partition1, partition11);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore))
        .contains(recoveryTask1, recoveryTask2);
    assertThat(recoveryTaskCreator.determineStartingOffset(1650, 0, indexerConfig)).isEqualTo(1650);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsExactlyInAnyOrder(partition1, partition11);
    List<RecoveryTaskMetadata> recoveryTasks3 =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore);
    assertThat(recoveryTasks3.size()).isEqualTo(3);
    RecoveryTaskMetadata recoveryTask3 =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore).stream()
            .filter(r -> !r.equals(recoveryTask1) && !r.equals(recoveryTask2))
            .findFirst()
            .get();
    assertThat(recoveryTask3.partitionId).isEqualTo(partitionId);
    assertThat(recoveryTask3.startOffset).isEqualTo(1450);
    assertThat(recoveryTask3.endOffset).isEqualTo(1649);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore))
        .containsExactlyInAnyOrder(recoveryTask1, recoveryTask2, recoveryTask3);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(3);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(4);

    // Live partitions from multiple partitions exist.
    snapshotMetadataStore.createSync(livePartition1);
    snapshotMetadataStore.createSync(livePartition11);
    SnapshotMetadata livePartition2 =
        new SnapshotMetadata(
            name + "2", LIVE_SNAPSHOT_PATH, startTime, endTime, maxOffset * 5, "2", LOGS_LUCENE9);
    snapshotMetadataStore.createSync(livePartition2);
    await()
        .until(
            () ->
                snapshotMetadataStore
                    .listSync()
                    .containsAll(List.of(livePartition1, livePartition11, livePartition2)));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .contains(partition1, partition11, livePartition1, livePartition2);
    assertThat(recoveryTaskCreator.determineStartingOffset(1660, 0, indexerConfig)).isEqualTo(1650);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore))
        .containsExactlyInAnyOrder(recoveryTask1, recoveryTask2, recoveryTask3);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsExactlyInAnyOrder(partition1, partition11, livePartition2);
    assertThat(recoveryTaskCreator.determineStartingOffset(1850, 0, indexerConfig)).isEqualTo(1850);
    List<RecoveryTaskMetadata> recoveryTasks4 =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore);
    assertThat(recoveryTasks4.size()).isEqualTo(4);
    RecoveryTaskMetadata recoveryTask4 =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore).stream()
            .filter(r -> !recoveryTasks3.contains(r))
            .findFirst()
            .get();
    assertThat(recoveryTask4.partitionId).isEqualTo(partitionId);
    assertThat(recoveryTask4.startOffset).isEqualTo(1650);
    assertThat(recoveryTask4.endOffset).isEqualTo(1849);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsExactlyInAnyOrder(partition1, partition11, livePartition2);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore))
        .containsExactlyInAnyOrder(recoveryTask1, recoveryTask2, recoveryTask3, recoveryTask4);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(5);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(5);

    // Live and non-live partitions for different partitions exist.
    snapshotMetadataStore.createSync(livePartition1);
    snapshotMetadataStore.createSync(livePartition11);
    SnapshotMetadata partition2 =
        new SnapshotMetadata(
            name + "3", path, startTime, endTime, maxOffset * 3, "2", LOGS_LUCENE9);
    snapshotMetadataStore.createSync(partition2);
    await()
        .until(
            () ->
                snapshotMetadataStore
                    .listSync()
                    .containsAll(List.of(livePartition1, livePartition11, livePartition2)));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .contains(partition1, partition11, livePartition1, livePartition2, partition2);
    final RecoveryTaskMetadata recoveryTaskPartition2 =
        new RecoveryTaskMetadata("basicRecovery" + "2", "2", 10000, 20000, 1000);
    recoveryTaskStore.createSync(recoveryTaskPartition2);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(1650, 0, indexerConfig));
    assertThat(recoveryTaskCreator.determineStartingOffset(1900, 0, indexerConfig)).isEqualTo(1850);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore))
        .containsExactlyInAnyOrder(
            recoveryTask1, recoveryTask2, recoveryTask3, recoveryTask4, recoveryTaskPartition2);
    assertThat(recoveryTaskCreator.determineStartingOffset(2050, 0, indexerConfig)).isEqualTo(2050);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore).size()).isEqualTo(6);
    RecoveryTaskMetadata recoveryTask5 =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore).stream()
            .filter(r -> !recoveryTasks4.contains(r) && !r.equals(recoveryTaskPartition2))
            .findFirst()
            .get();
    assertThat(recoveryTask5.partitionId).isEqualTo(partitionId);
    assertThat(recoveryTask5.startOffset).isEqualTo(1850);
    assertThat(recoveryTask5.endOffset).isEqualTo(2049);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsExactlyInAnyOrder(partition1, partition11, livePartition2, partition2);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore))
        .containsExactlyInAnyOrder(
            recoveryTask1,
            recoveryTask2,
            recoveryTask3,
            recoveryTask4,
            recoveryTask5,
            recoveryTaskPartition2);
    assertThat(getCount(STALE_SNAPSHOT_DELETE_SUCCESS, meterRegistry)).isEqualTo(7);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(6);
  }

  @Test
  public void testRecoveryTaskCreationFailureFailsDetermineStartOffset() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000, 0, indexerConfig)).isNegative();

    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId, LOGS_LUCENE9);
    snapshotMetadataStore.createSync(partition1);
    await().until(() -> snapshotMetadataStore.listSync().contains(partition1));
    assertThat(
            getHighestDurableOffsetForPartition(
                KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore),
                Collections.emptyList(),
                partitionId))
        .isEqualTo(100);
    assertThat(recoveryTaskCreator.determineStartingOffset(1150, 0, indexerConfig)).isEqualTo(1150);
    List<RecoveryTaskMetadata> recoveryTasks1 =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore);
    assertThat(recoveryTasks1.size()).isEqualTo(1);
    assertThat(recoveryTasks1.get(0).startOffset).isEqualTo(101);
    assertThat(recoveryTasks1.get(0).endOffset).isEqualTo(1149);
    assertThat(recoveryTasks1.get(0).partitionId).isEqualTo(partitionId);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(50, 0, indexerConfig));

    // Fail a recovery store task creation.
    doThrow(new RuntimeException()).when(recoveryTaskStore).createSync(any());

    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(1350, 0, indexerConfig));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore))
        .containsExactly(recoveryTasks1.get(0));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).contains(partition1);
  }

  @Test
  public void testSnapshotListFailureFailsDetermineStartOffset() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000, 0, indexerConfig)).isNegative();

    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId, LOGS_LUCENE9);
    snapshotMetadataStore.createSync(partition1);
    await().until(() -> snapshotMetadataStore.listSync().contains(partition1));
    assertThat(
            getHighestDurableOffsetForPartition(
                KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore),
                Collections.emptyList(),
                partitionId))
        .isEqualTo(100);
    assertThat(recoveryTaskCreator.determineStartingOffset(1150, 0, indexerConfig)).isEqualTo(1150);
    List<RecoveryTaskMetadata> recoveryTasks1 =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore);
    assertThat(recoveryTasks1.size()).isEqualTo(1);
    assertThat(recoveryTasks1.get(0).startOffset).isEqualTo(101);
    assertThat(recoveryTasks1.get(0).endOffset).isEqualTo(1149);
    assertThat(recoveryTasks1.get(0).partitionId).isEqualTo(partitionId);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(50, 0, indexerConfig));

    // Fail snapshot store list creation.
    doThrow(new InternalMetadataStoreException(""))
        .doCallRealMethod()
        .when(snapshotMetadataStore)
        .listSync();

    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(1350, 0, indexerConfig));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore))
        .containsExactly(recoveryTasks1.get(0));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsExactly(partition1);
  }

  @Test
  public void testRecoveryListFailureFailsDetermineStartOffset() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000, 0, indexerConfig)).isNegative();

    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId, LOGS_LUCENE9);
    snapshotMetadataStore.createSync(partition1);
    await().until(() -> snapshotMetadataStore.listSync().contains(partition1));
    assertThat(
            getHighestDurableOffsetForPartition(
                KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore),
                Collections.emptyList(),
                partitionId))
        .isEqualTo(100);
    assertThat(recoveryTaskCreator.determineStartingOffset(1150, 0, indexerConfig)).isEqualTo(1150);
    List<RecoveryTaskMetadata> recoveryTasks1 =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore);
    assertThat(recoveryTasks1.size()).isEqualTo(1);
    assertThat(recoveryTasks1.get(0).startOffset).isEqualTo(101);
    assertThat(recoveryTasks1.get(0).endOffset).isEqualTo(1149);
    assertThat(recoveryTasks1.get(0).partitionId).isEqualTo(partitionId);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(50, 0, indexerConfig));

    // Fail a recovery store list.
    doThrow(new InternalMetadataStoreException(""))
        .doCallRealMethod()
        .when(recoveryTaskStore)
        .listSync();

    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(1350, 0, indexerConfig));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore))
        .containsExactly(recoveryTasks1.get(0));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsExactly(partition1);
  }

  @Test
  public void testFailureToDeleteStaleSnapshotsFailsDetermineStartOffset()
      throws ExecutionException, InterruptedException {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            TEST_MAX_MESSAGES_PER_RECOVERY_TASK,
            meterRegistry);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000, 0, indexerConfig)).isNegative();

    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId, LOGS_LUCENE9);
    snapshotMetadataStore.createSync(partition1);
    await().until(() -> snapshotMetadataStore.listSync().contains(partition1));
    assertThat(
            getHighestDurableOffsetForPartition(
                KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore),
                Collections.emptyList(),
                partitionId))
        .isEqualTo(100);
    assertThat(recoveryTaskCreator.determineStartingOffset(1150, 0, indexerConfig)).isEqualTo(1150);
    List<RecoveryTaskMetadata> recoveryTasks1 =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore);
    assertThat(recoveryTasks1.size()).isEqualTo(1);
    assertThat(recoveryTasks1.get(0).startOffset).isEqualTo(101);
    assertThat(recoveryTasks1.get(0).endOffset).isEqualTo(1149);
    assertThat(recoveryTasks1.get(0).partitionId).isEqualTo(partitionId);
    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(50, 0, indexerConfig));

    // Add a live partition to be deleted.
    SnapshotMetadata livePartition1 =
        new SnapshotMetadata(
            name + "1",
            LIVE_SNAPSHOT_PATH,
            startTime,
            endTime,
            maxOffset,
            partitionId,
            LOGS_LUCENE9);
    snapshotMetadataStore.createSync(livePartition1);
    await().until(() -> snapshotMetadataStore.listSync().contains(livePartition1));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsExactlyInAnyOrder(partition1, livePartition1);
    // Fail deletion on snapshot metadata store.
    doThrow(new IllegalStateException())
        .when(snapshotMetadataStore)
        .deleteAsync(any(SnapshotMetadata.class));
    doThrow(new IllegalStateException()).when(snapshotMetadataStore).deleteAsync(any());

    assertThatIllegalStateException()
        .isThrownBy(() -> recoveryTaskCreator.determineStartingOffset(1350, 0, indexerConfig));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore))
        .containsExactly(recoveryTasks1.get(0));
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore))
        .containsExactlyInAnyOrder(partition1, livePartition1);
  }

  @Test
  public void testChunkingRecoveryTaskLongRange() {
    final long maxMessagesPerRecoveryTask = 100;
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            maxMessagesPerRecoveryTask,
            meterRegistry);

    // Long range - 1 chunk
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    recoveryTaskCreator.createRecoveryTasks("1", 10, 100, maxMessagesPerRecoveryTask);
    List<RecoveryTaskMetadata> recoveryTasks =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore);
    assertThat(recoveryTasks.size()).isEqualTo(1);
    assertThat(recoveryTasks.get(0).startOffset).isEqualTo(10);
    assertThat(recoveryTasks.get(0).endOffset).isEqualTo(100);
    assertThat(recoveryTasks.get(0).partitionId).isEqualTo(partitionId);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(1);
  }

  @Test
  public void testChunkingRecoveryOneChunk() {
    final long maxMessagesPerRecoveryTask = 100;
    final String testPartitionId = "2";
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            testPartitionId,
            100,
            maxMessagesPerRecoveryTask,
            meterRegistry);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    recoveryTaskCreator.createRecoveryTasks(testPartitionId, 100, 101, maxMessagesPerRecoveryTask);
    List<RecoveryTaskMetadata> recoveryTasks =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore);
    assertThat(recoveryTasks.size()).isEqualTo(1);
    assertThat(recoveryTasks.get(0).startOffset).isEqualTo(100);
    assertThat(recoveryTasks.get(0).endOffset).isEqualTo(101);
    assertThat(recoveryTasks.get(0).partitionId).isEqualTo(testPartitionId);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(1);
  }

  @Test
  public void testChunkingRecoveryTasksSameSizeTasks() {
    final String testPartitionId = "3";
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore, recoveryTaskStore, testPartitionId, 100, 2, meterRegistry);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    recoveryTaskCreator.createRecoveryTasks(testPartitionId, 100, 105, 2);
    List<RecoveryTaskMetadata> recoveryTasks =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore);
    assertThat(recoveryTasks.size()).isEqualTo(3);
    assertThat(recoveryTasks.stream().mapToLong(r -> r.startOffset).sorted().toArray())
        .containsExactly(100, 102, 104);
    assertThat(recoveryTasks.stream().mapToLong(r -> r.endOffset).sorted().toArray())
        .containsExactly(101, 103, 105);
    assertThat(recoveryTasks.stream().mapToLong(r -> r.endOffset - r.startOffset).toArray())
        .containsExactly(1, 1, 1);
    assertThat(recoveryTasks.stream().filter(r -> r.partitionId.equals(testPartitionId)).count())
        .isEqualTo(3);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(3);
  }

  @Test
  public void testChunkingRecoveryTasksWithOddSizeTask() {
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore, recoveryTaskStore, partitionId, 100, 2, meterRegistry);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    recoveryTaskCreator.createRecoveryTasks(partitionId, 100, 104, 2);
    List<RecoveryTaskMetadata> recoveryTasks =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore);
    assertThat(recoveryTasks.size()).isEqualTo(3);
    assertThat(recoveryTasks.stream().mapToLong(r -> r.startOffset).sorted().toArray())
        .containsExactly(100, 102, 104);
    assertThat(recoveryTasks.stream().mapToLong(r -> r.endOffset).sorted().toArray())
        .containsExactly(101, 103, 104);
    assertThat(
            recoveryTasks.stream().mapToLong(r -> r.endOffset - r.startOffset).sorted().toArray())
        .containsExactly(0, 1, 1);
    assertThat(recoveryTasks.stream().filter(r -> r.partitionId.equals(partitionId)).count())
        .isEqualTo(3);
    assertThat(getCount(RECOVERY_TASKS_CREATED, meterRegistry)).isEqualTo(3);
  }

  @Test
  public void testMultipleRecoveryTaskCreationWithSnapshotDelay() {
    final long maxMessagesPerRecoveryTask = 500;
    RecoveryTaskCreator recoveryTaskCreator =
        new RecoveryTaskCreator(
            snapshotMetadataStore,
            recoveryTaskStore,
            partitionId,
            100,
            maxMessagesPerRecoveryTask,
            meterRegistry);

    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).isEmpty();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();
    // When there is no data return -1.
    assertThat(recoveryTaskCreator.determineStartingOffset(1000, 0, indexerConfig)).isNegative();
    assertThat(KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore)).isEmpty();

    final String name = "testSnapshotId";
    final String path = "/testPath_" + name;
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 100;

    final SnapshotMetadata partition1 =
        new SnapshotMetadata(name, path, startTime, endTime, maxOffset, partitionId, LOGS_LUCENE9);
    snapshotMetadataStore.createSync(partition1);
    await().until(() -> snapshotMetadataStore.listSync().contains(partition1));
    assertThat(
            getHighestDurableOffsetForPartition(
                KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore),
                Collections.emptyList(),
                partitionId))
        .isEqualTo(100);
    assertThat(recoveryTaskCreator.determineStartingOffset(1150, 0, indexerConfig)).isEqualTo(1150);
    List<RecoveryTaskMetadata> recoveryTasks1 =
        KaldbMetadataTestUtils.listSyncUncached(recoveryTaskStore);
    assertThat(recoveryTasks1.size()).isEqualTo(3);
    assertThat(recoveryTasks1.stream().mapToLong(r -> r.startOffset).sorted().toArray())
        .containsExactly(101, 601, 1101);
    assertThat(recoveryTasks1.stream().mapToLong(r -> r.endOffset).sorted().toArray())
        .containsExactly(600, 1100, 1149);
    assertThat(
            recoveryTasks1.stream().mapToLong(r -> r.endOffset - r.startOffset).sorted().toArray())
        .containsExactly(48, 499, 499);
    assertThat(recoveryTasks1.stream().filter(r -> r.partitionId.equals(partitionId)).count())
        .isEqualTo(3);
  }
}
