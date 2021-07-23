package com.slack.kaldb.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataSerializer;
import com.slack.kaldb.metadata.zookeeper.InternalMetadataStoreException;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.NoNodeException;
import com.slack.kaldb.metadata.zookeeper.NodeExistsException;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.util.CountingFatalErrorHandler;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Enclosed.class)
public class KaldbMetadataStoreTest {
  static SnapshotMetadata makeSnapshot(String name) {
    return makeSnapshot(name, 100);
  }

  static SnapshotMetadata makeSnapshot(String name, long maxOffset) {
    final String snapshotPath = "s3://snapshots/path";
    final String snapshotId = name + "Id";
    final long startTimeUtc = 12345;
    final long endTimeUtc = 123456;
    final String partitionId = "1";

    return new SnapshotMetadata(
        name, snapshotPath, snapshotId, startTimeUtc, endTimeUtc, maxOffset, partitionId);
  }

  public static class TestPersistentCreatableUpdatableCacheableMetadataStore {
    private static class DummyPersistentCreatableUpdatableCacheableMetadataStore
        extends PersistentMutableMetadataStore<SnapshotMetadata> {
      public DummyPersistentCreatableUpdatableCacheableMetadataStore(
          String storeFolder,
          MetadataStore metadataStore,
          MetadataSerializer<SnapshotMetadata> metadataSerializer,
          Logger logger)
          throws Exception {
        super(true, true, storeFolder, metadataStore, metadataSerializer, logger);
      }
    }

    private static final Logger LOG =
        LoggerFactory.getLogger(DummyPersistentCreatableUpdatableCacheableMetadataStore.class);

    private TestingServer testingServer;
    private ZookeeperMetadataStoreImpl zkMetadataStore;
    private MeterRegistry meterRegistry;
    private DummyPersistentCreatableUpdatableCacheableMetadataStore store;

    @Before
    public void setUp() throws Exception {
      meterRegistry = new SimpleMeterRegistry();
      // NOTE: Sometimes the ZK server fails to start. Handle it more gracefully, if tests are
      // flaky.
      testingServer = new TestingServer();
      CountingFatalErrorHandler countingFatalErrorHandler = new CountingFatalErrorHandler();
      zkMetadataStore =
          new ZookeeperMetadataStoreImpl(
              testingServer.getConnectString(),
              "test",
              1000,
              1000,
              new RetryNTimes(1, 500),
              countingFatalErrorHandler,
              meterRegistry);
      this.store =
          new DummyPersistentCreatableUpdatableCacheableMetadataStore(
              "/snapshots", zkMetadataStore, new SnapshotMetadataSerializer(), LOG);
    }

    @After
    public void tearDown() throws IOException {
      zkMetadataStore.close();
      testingServer.close();
      meterRegistry.close();
    }

    @Test
    public void testCreateGetDelete() throws ExecutionException, InterruptedException {
      final String name = "testSnapshot";
      final String snapshotPath = "s3://snapshots/path";
      final String snapshotId = "testSnapshotId";
      final long startTimeUtc = 12345;
      final long endTimeUtc = 123456;
      final long maxOffset = 100;
      final String partitionId = "1";

      final SnapshotMetadata snapshot =
          new SnapshotMetadata(
              name, snapshotPath, snapshotId, startTimeUtc, endTimeUtc, maxOffset, partitionId);

      assertThat(store.list().get()).isEmpty();
      assertThat(store.create(snapshot).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.list().get()).containsOnly(snapshot);

      SnapshotMetadata metadata = store.get(name).get();
      assertThat(metadata.name).isEqualTo(name);
      assertThat(metadata.snapshotPath).isEqualTo(snapshotPath);
      assertThat(metadata.snapshotId).isEqualTo(snapshotId);
      assertThat(metadata.startTimeUtc).isEqualTo(startTimeUtc);
      assertThat(metadata.endTimeUtc).isEqualTo(endTimeUtc);
      assertThat(metadata.maxOffset).isEqualTo(maxOffset);
      assertThat(metadata.partitionId).isEqualTo(partitionId);

      String newSnapshotId = "newSnapshotId";
      final SnapshotMetadata newSnapshot =
          new SnapshotMetadata(
              name,
              snapshotPath,
              newSnapshotId,
              startTimeUtc + 1,
              endTimeUtc + 1,
              maxOffset + 100,
              partitionId);
      assertThat(store.update(newSnapshot).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.list().get()).containsOnly(newSnapshot);
      SnapshotMetadata newMetadata = store.get(name).get();
      assertThat(newMetadata.name).isEqualTo(name);
      assertThat(newMetadata.snapshotPath).isEqualTo(snapshotPath);
      assertThat(newMetadata.snapshotId).isEqualTo(newSnapshotId);
      assertThat(newMetadata.startTimeUtc).isEqualTo(startTimeUtc + 1);
      assertThat(newMetadata.endTimeUtc).isEqualTo(endTimeUtc + 1);
      assertThat(newMetadata.maxOffset).isEqualTo(maxOffset + 100);
      assertThat(newMetadata.partitionId).isEqualTo(partitionId);

      assertThat(store.delete(name).get()).isNull();
      assertThat(store.list().get()).isEmpty();
    }

    @Test
    public void testMultipleCreates() throws ExecutionException, InterruptedException {
      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      final String name2 = "snapshot2";
      SnapshotMetadata snapshot2 = makeSnapshot(name2);

      assertThat(store.list().get()).isEmpty();
      assertThat(store.create(snapshot1).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.create(snapshot2).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(2);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);

      SnapshotMetadata newSnapshot1 = makeSnapshot(name1, 300);
      assertThat(store.update(newSnapshot1).get()).isNull();
      assertThat(store.list().get()).containsOnly(newSnapshot1, snapshot2);

      // Adding a snapshot with the same name but different values throws exception.
      SnapshotMetadata duplicateSnapshot2 = makeSnapshot(name2, 300);
      Throwable duplicateEx = catchThrowable(() -> store.create(duplicateSnapshot2).get());
      assertThat(duplicateEx.getCause()).isInstanceOf(NodeExistsException.class);

      assertThat(store.delete(name2).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.list().get()).containsOnly(newSnapshot1);

      assertThat(store.delete(name1).get()).isNull();
      assertThat(store.list().get().isEmpty()).isTrue();

      Throwable deleteEx = catchThrowable(() -> store.delete(name1).get());
      assertThat(deleteEx.getCause()).isInstanceOf(NoNodeException.class);
    }

    @Test
    public void testDuplicateCreateNode() throws ExecutionException, InterruptedException {
      final String name = "testSnapshot";
      final String snapshotPath = "s3://snapshots/path";
      final String snapshotId = "testSnapshotId";
      final long startTimeUtc = 12345;
      final long endTimeUtc = 123456;
      final long maxOffset = 100;
      final String partitionId = "1";

      final SnapshotMetadata testSnapshot =
          new SnapshotMetadata(
              name, snapshotPath, snapshotId, startTimeUtc, endTimeUtc, maxOffset, partitionId);
      assertThat(store.create(testSnapshot).get()).isNull();

      SnapshotMetadata metadata = store.get(name).get();
      assertThat(metadata.name).isEqualTo(name);
      assertThat(metadata.snapshotPath).isEqualTo(snapshotPath);
      assertThat(metadata.snapshotId).isEqualTo(snapshotId);
      assertThat(metadata.startTimeUtc).isEqualTo(startTimeUtc);
      assertThat(metadata.endTimeUtc).isEqualTo(endTimeUtc);
      assertThat(metadata.maxOffset).isEqualTo(maxOffset);
      assertThat(metadata.partitionId).isEqualTo(partitionId);

      Throwable duplicateCreateEx = catchThrowable(() -> store.create(testSnapshot).get());
      assertThat(duplicateCreateEx.getCause()).isInstanceOf(NodeExistsException.class);

      assertThat(store.delete(name).get()).isNull();

      Throwable getMissingNodeEx = catchThrowable(() -> store.get(name).get());
      assertThat(getMissingNodeEx.getCause()).isInstanceOf(NoNodeException.class);

      Throwable deleteMissingNodeEx = catchThrowable(() -> store.delete(name).get());
      assertThat(deleteMissingNodeEx.getCause()).isInstanceOf(NoNodeException.class);
    }

    @Test
    public void testStoreOperationsOnStoppedServer()
        throws ExecutionException, InterruptedException, IOException {
      assertThat(store.list().get().isEmpty()).isTrue();

      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      assertThat(store.create(snapshot1).get()).isNull();
      assertThat(store.list().get()).containsOnly(snapshot1);
      assertThat(store.list().get().size()).isEqualTo(1);

      // Stop the ZK server
      testingServer.stop();

      Throwable createEx = catchThrowable(() -> store.create(snapshot1).get());
      assertThat(createEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

      Throwable updateEx = catchThrowable(() -> store.update(snapshot1).get());
      assertThat(updateEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

      Throwable deleteEx = catchThrowable(() -> store.delete(name1).get());
      assertThat(deleteEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

      Throwable listEx = catchThrowable(() -> store.list().get());
      assertThat(listEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

      Throwable getEx = catchThrowable(() -> store.get(name1).get());
      assertThat(getEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);
    }

    @Test
    public void testNotificationFiresOnCreate() throws ExecutionException, InterruptedException {
      assertThat(store.list().get().isEmpty()).isTrue();

      CountDownLatch notificationCountDownLatch = new CountDownLatch(1);
      AtomicInteger notificationCounter = new AtomicInteger(0);
      final KaldbMetadataStoreChangeListener testListener =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };

      store.addListener(testListener);

      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      assertThat(store.create(snapshot1).get()).isNull();
      notificationCountDownLatch.await();
      assertThat(store.getCached()).containsOnly(snapshot1);
      assertThat(store.list().get()).containsOnly(snapshot1);
      assertThat(notificationCounter.get()).isEqualTo(1);
    }

    @Test
    public void testNotificationFiresOnDataChange()
        throws ExecutionException, InterruptedException {
      assertThat(store.list().get().isEmpty()).isTrue();

      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      assertThat(store.create(snapshot1).get()).isNull();
      await().until(() -> store.getCached().size() == 1);
      assertThat(store.getCached()).containsOnly(snapshot1);
      assertThat(store.list().get()).containsOnly(snapshot1);

      CountDownLatch notificationCountDownLatch = new CountDownLatch(1);
      AtomicInteger notificationCounter = new AtomicInteger(0);

      final KaldbMetadataStoreChangeListener testListener =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };

      store.addListener(testListener);

      SnapshotMetadata newSnapshot1 = makeSnapshot(name1, 30000);

      assertThat(store.update(newSnapshot1).get()).isNull();
      notificationCountDownLatch.await();
      await().until(() -> store.getCached().contains(newSnapshot1));
      assertThat(store.getCached()).containsOnly(newSnapshot1);
      assertThat(store.list().get()).containsOnly(newSnapshot1);
      assertThat(notificationCounter.get()).isEqualTo(1);
    }

    @Test
    public void testNotificationFiresOnRemove() throws ExecutionException, InterruptedException {
      assertThat(store.list().get().isEmpty()).isTrue();

      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      assertThat(store.create(snapshot1).get()).isNull();
      await().until(() -> store.getCached().size() == 1);
      assertThat(store.getCached()).containsOnly(snapshot1);
      assertThat(store.list().get()).containsOnly(snapshot1);

      CountDownLatch notificationCountDownLatch = new CountDownLatch(1);
      AtomicInteger notificationCounter = new AtomicInteger(0);

      final KaldbMetadataStoreChangeListener testListener =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };

      store.addListener(testListener);

      assertThat(store.delete(name1).get()).isNull();
      notificationCountDownLatch.await();
      assertThat(store.getCached().isEmpty()).isTrue();
      assertThat(store.list().get().isEmpty()).isTrue();
      assertThat(notificationCounter.get()).isEqualTo(1);
    }

    @Test
    public void testMultipleWatchersOnMetadataStore()
        throws ExecutionException, InterruptedException {
      assertThat(store.list().get().isEmpty()).isTrue();

      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      assertThat(store.create(snapshot1).get()).isNull();
      await().until(() -> store.getCached().size() == 1);
      assertThat(store.getCached()).containsOnly(snapshot1);
      assertThat(store.list().get()).containsOnly(snapshot1);

      final CountDownLatch notificationCountDownLatch = new CountDownLatch(1);
      final AtomicInteger notificationCounter = new AtomicInteger(0);
      final KaldbMetadataStoreChangeListener testListener1 =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };
      store.addListener(testListener1);

      final CountDownLatch notificationCountDownLatch2 = new CountDownLatch(1);
      final KaldbMetadataStoreChangeListener testListener2 =
          () -> {
            notificationCountDownLatch2.countDown();
            notificationCounter.incrementAndGet();
          };
      store.addListener(testListener2);

      final String name2 = "snapshot2";
      SnapshotMetadata snapshot2 = makeSnapshot(name2);
      assertThat(store.create(snapshot2).get()).isNull();
      notificationCountDownLatch.await();
      notificationCountDownLatch2.await();
      assertThat(store.getCached()).containsOnly(snapshot1, snapshot2);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);
      assertThat(store.get(name2).get()).isEqualTo(snapshot2);
      assertThat(notificationCounter.get()).isEqualTo(2);
    }

    @Test
    public void testThrowingListenerOnMetadataStore()
        throws ExecutionException, InterruptedException {
      assertThat(store.list().get().isEmpty()).isTrue();

      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      assertThat(store.create(snapshot1).get()).isNull();
      await().until(() -> store.getCached().size() == 1);
      assertThat(store.getCached()).containsOnly(snapshot1);
      assertThat(store.list().get()).containsOnly(snapshot1);

      final CountDownLatch notificationCountDownLatch = new CountDownLatch(3);
      final AtomicInteger notificationCounter = new AtomicInteger(0);

      final KaldbMetadataStoreChangeListener regularListener =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };
      store.addListener(regularListener);

      final KaldbMetadataStoreChangeListener throwingListener =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
            throw new RuntimeException("test exception");
          };
      store.addListener(throwingListener);

      final KaldbMetadataStoreChangeListener regularListener2 =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };
      store.addListener(regularListener2);

      final String name2 = "snapshot2";
      SnapshotMetadata snapshot2 = makeSnapshot(name2);
      assertThat(store.create(snapshot2).get()).isNull();
      notificationCountDownLatch.await();
      assertThat(store.getCached()).containsOnly(snapshot1, snapshot2);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);
      assertThat(store.get(name2).get()).isEqualTo(snapshot2);
      assertThat(notificationCounter.get()).isEqualTo(3);
    }

    @Test
    public void testRemoveListenerOnMetadataStore()
        throws ExecutionException, InterruptedException {
      assertThat(store.list().get().isEmpty()).isTrue();

      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      assertThat(store.create(snapshot1).get()).isNull();
      await().until(() -> store.getCached().size() == 1);
      assertThat(store.getCached()).containsOnly(snapshot1);
      assertThat(store.list().get()).containsOnly(snapshot1);

      final CountDownLatch notificationCountDownLatch = new CountDownLatch(3);
      final AtomicInteger notificationCounter = new AtomicInteger(0);

      final KaldbMetadataStoreChangeListener regularListener =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };
      store.addListener(regularListener);

      final KaldbMetadataStoreChangeListener regularListener2 =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };
      store.addListener(regularListener2);

      final String name2 = "snapshot2";
      SnapshotMetadata snapshot2 = makeSnapshot(name2);
      assertThat(store.create(snapshot2).get()).isNull();
      await().until(() -> store.getCached().contains(snapshot2));
      assertThat(store.getCached()).containsOnly(snapshot1, snapshot2);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);
      assertThat(store.get(name2).get()).isEqualTo(snapshot2);
      assertThat(notificationCounter.get()).isEqualTo(2);

      // Remove listener
      store.removeListener(regularListener2);
      assertThat(store.delete(name2).get()).isNull();
      notificationCountDownLatch.await();
      assertThat(store.getCached()).containsOnly(snapshot1);
      assertThat(store.list().get()).containsOnly(snapshot1);
      assertThat(store.get(name1).get()).isEqualTo(snapshot1);
      assertThat(notificationCounter.get()).isEqualTo(3);

      Throwable getEx = catchThrowable(() -> store.get(name2).get());
      assertThat(getEx.getCause()).isInstanceOf(NoNodeException.class);
    }

    @Test
    public void testCorruptZkMetadata() throws ExecutionException, InterruptedException {
      assertThat(store.list().get().isEmpty()).isTrue();

      final CountDownLatch notificationCountDownLatch = new CountDownLatch(2);
      final AtomicInteger notificationCounter = new AtomicInteger(0);
      final KaldbMetadataStoreChangeListener regularListener =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };
      store.addListener(regularListener);

      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      assertThat(store.create(snapshot1).get()).isNull();
      await().until(() -> store.getCached().size() == 1);
      assertThat(store.getCached()).containsOnly(snapshot1);
      assertThat(store.list().get()).containsOnly(snapshot1);

      final String name2 = "snapshot2";
      SnapshotMetadata snapshot2 = makeSnapshot(name2);
      assertThat(store.create(snapshot2).get()).isNull();
      notificationCountDownLatch.await();
      await().until(() -> store.getCached().size() == 2);
      assertThat(store.getCached()).containsOnly(snapshot1, snapshot2);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);

      // Corrupt the metadata store.
      assertThat(zkMetadataStore.put("/snapshots/" + name1, "corrupt").get()).isNull();

      // Get throws exception but store is fine.
      Throwable getEx = catchThrowable(() -> store.get(name1).get());
      assertThat(getEx.getCause()).isInstanceOf(CorruptMetadataNodeException.class);
      assertThat(store.get(name2).get()).isEqualTo(snapshot2);

      assertThat(store.getCached()).containsOnly(snapshot2);
      assertThat(store.list().get()).containsOnly(null, snapshot2);
    }
  }

  public static class TestPersistentCreatableCacheableMetadataStore {
    private static class DummyPersistentCreatableCacheableMetadataStore
        extends PersistentMutableMetadataStore<SnapshotMetadata> {
      public DummyPersistentCreatableCacheableMetadataStore(
          String storeFolder,
          MetadataStore metadataStore,
          MetadataSerializer<SnapshotMetadata> metadataSerializer,
          Logger logger)
          throws Exception {
        super(true, false, storeFolder, metadataStore, metadataSerializer, logger);
      }
    }

    private static final Logger LOG =
        LoggerFactory.getLogger(TestPersistentCreatableCacheableMetadataStore.class);

    private TestingServer testingServer;
    private ZookeeperMetadataStoreImpl zkMetadataStore;
    private MeterRegistry meterRegistry;
    private DummyPersistentCreatableCacheableMetadataStore store;

    @Before
    public void setUp() throws Exception {
      meterRegistry = new SimpleMeterRegistry();
      // NOTE: Sometimes the ZK server fails to start. Handle it more gracefully, if tests are
      // flaky.
      testingServer = new TestingServer();
      CountingFatalErrorHandler countingFatalErrorHandler = new CountingFatalErrorHandler();
      zkMetadataStore =
          new ZookeeperMetadataStoreImpl(
              testingServer.getConnectString(),
              "test",
              1000,
              1000,
              new RetryNTimes(1, 500),
              countingFatalErrorHandler,
              meterRegistry);
      this.store =
          new DummyPersistentCreatableCacheableMetadataStore(
              "/snapshots", zkMetadataStore, new SnapshotMetadataSerializer(), LOG);
    }

    @After
    public void tearDown() throws IOException {
      zkMetadataStore.close();
      testingServer.close();
      meterRegistry.close();
    }

    @Test
    public void testCrudOperations() throws ExecutionException, InterruptedException {
      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      final String name2 = "snapshot2";
      SnapshotMetadata snapshot2 = makeSnapshot(name2);

      assertThat(store.list().get()).isEmpty();
      assertThat(store.create(snapshot1).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.create(snapshot2).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(2);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);
      assertThat(store.getCached()).containsOnly(snapshot1, snapshot2);

      // Updates throw an exception.
      SnapshotMetadata newSnapshot1 = makeSnapshot(name1, 300);
      Throwable updateEx = catchThrowable(() -> store.update(newSnapshot1).get());
      assertThat(updateEx).isInstanceOf(UnsupportedOperationException.class);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);
      assertThat(store.getCached()).containsOnly(snapshot1, snapshot2);

      // Adding a snapshot with the same name but different values throws exception.
      SnapshotMetadata duplicateSnapshot2 = makeSnapshot(name2, 300);
      Throwable duplicateEx = catchThrowable(() -> store.create(duplicateSnapshot2).get());
      assertThat(duplicateEx.getCause()).isInstanceOf(NodeExistsException.class);

      assertThat(store.delete(name2).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.list().get()).containsOnly(snapshot1);
      assertThat(store.getCached()).containsOnly(snapshot1);

      assertThat(store.delete(name1).get()).isNull();
      assertThat(store.list().get().isEmpty()).isTrue();

      Throwable deleteEx = catchThrowable(() -> store.delete(name1).get());
      assertThat(deleteEx.getCause()).isInstanceOf(NoNodeException.class);
    }
  }

  public static class TestPersistentCreatableMetadataStore {
    private static class DummyPersistentCreatableMetadataStore
        extends PersistentMutableMetadataStore<SnapshotMetadata> {
      public DummyPersistentCreatableMetadataStore(
          String storeFolder,
          MetadataStore metadataStore,
          MetadataSerializer<SnapshotMetadata> metadataSerializer,
          Logger logger)
          throws Exception {
        super(false, false, storeFolder, metadataStore, metadataSerializer, logger);
      }
    }

    private static final Logger LOG =
        LoggerFactory.getLogger(TestPersistentCreatableCacheableMetadataStore.class);

    private TestingServer testingServer;
    private ZookeeperMetadataStoreImpl zkMetadataStore;
    private MeterRegistry meterRegistry;
    private DummyPersistentCreatableMetadataStore store;

    @Before
    public void setUp() throws Exception {
      meterRegistry = new SimpleMeterRegistry();
      // NOTE: Sometimes the ZK server fails to start. Handle it more gracefully, if tests are
      // flaky.
      testingServer = new TestingServer();
      CountingFatalErrorHandler countingFatalErrorHandler = new CountingFatalErrorHandler();
      zkMetadataStore =
          new ZookeeperMetadataStoreImpl(
              testingServer.getConnectString(),
              "test",
              1000,
              1000,
              new RetryNTimes(1, 500),
              countingFatalErrorHandler,
              meterRegistry);
      this.store =
          new DummyPersistentCreatableMetadataStore(
              "/snapshots", zkMetadataStore, new SnapshotMetadataSerializer(), LOG);
    }

    @After
    public void tearDown() throws IOException {
      zkMetadataStore.close();
      testingServer.close();
      meterRegistry.close();
    }

    @Test
    public void testCrudOperationsAndCache() throws ExecutionException, InterruptedException {
      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      final String name2 = "snapshot2";
      SnapshotMetadata snapshot2 = makeSnapshot(name2);

      assertThat(store.list().get()).isEmpty();
      assertThat(store.create(snapshot1).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.create(snapshot2).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(2);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);

      // Caching is disabled.
      Throwable getCacheEx = catchThrowable(() -> store.getCached());
      assertThat(getCacheEx).isInstanceOf(UnsupportedOperationException.class);

      // Updates throw an exception.
      SnapshotMetadata newSnapshot1 = makeSnapshot(name1, 300);
      Throwable updateEx = catchThrowable(() -> store.update(newSnapshot1).get());
      assertThat(updateEx).isInstanceOf(UnsupportedOperationException.class);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);

      // All cache operations are disabled.
      Throwable getCacheEx2 = catchThrowable(() -> store.getCached());
      KaldbMetadataStoreChangeListener listener = () -> {};
      assertThat(getCacheEx2).isInstanceOf(UnsupportedOperationException.class);
      Throwable addListenerEx = catchThrowable(() -> store.addListener(listener));
      assertThat(addListenerEx).isInstanceOf(UnsupportedOperationException.class);
      Throwable removeListenerEx = catchThrowable(() -> store.removeListener(listener));
      assertThat(removeListenerEx).isInstanceOf(UnsupportedOperationException.class);
      Throwable closeEx = catchThrowable(() -> store.close());
      assertThat(closeEx).isInstanceOf(UnsupportedOperationException.class);

      // Adding a snapshot with the same name but different values throws exception.
      SnapshotMetadata duplicateSnapshot2 = makeSnapshot(name2, 300);
      Throwable duplicateEx = catchThrowable(() -> store.create(duplicateSnapshot2).get());
      assertThat(duplicateEx.getCause()).isInstanceOf(NodeExistsException.class);

      assertThat(store.delete(name2).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.list().get()).containsOnly(snapshot1);

      assertThat(store.delete(name1).get()).isNull();
      assertThat(store.list().get().isEmpty()).isTrue();

      Throwable deleteEx = catchThrowable(() -> store.delete(name1).get());
      assertThat(deleteEx.getCause()).isInstanceOf(NoNodeException.class);
    }
  }

  public static class TestEphemeralCreatableUpdatableCacheableMetadataStore {
    private static class DummyEphemeralCreatableUpdatableCacheableMetadataStore
        extends EphemeralMutableMetadataStore<SnapshotMetadata> {
      public DummyEphemeralCreatableUpdatableCacheableMetadataStore(
          String storeFolder,
          MetadataStore metadataStore,
          MetadataSerializer<SnapshotMetadata> metadataSerializer,
          Logger logger)
          throws Exception {
        super(true, true, storeFolder, metadataStore, metadataSerializer, logger);
      }
    }

    private static final Logger LOG =
        LoggerFactory.getLogger(DummyEphemeralCreatableUpdatableCacheableMetadataStore.class);

    private TestingServer testingServer;
    private ZookeeperMetadataStoreImpl zkMetadataStore;
    private MeterRegistry meterRegistry;
    private DummyEphemeralCreatableUpdatableCacheableMetadataStore store;

    @Before
    public void setUp() throws Exception {
      meterRegistry = new SimpleMeterRegistry();
      // NOTE: Sometimes the ZK server fails to start. Handle it more gracefully, if tests are
      // flaky.
      testingServer = new TestingServer();
      CountingFatalErrorHandler countingFatalErrorHandler = new CountingFatalErrorHandler();
      zkMetadataStore =
          new ZookeeperMetadataStoreImpl(
              testingServer.getConnectString(),
              "test",
              1000,
              1000,
              new RetryNTimes(1, 500),
              countingFatalErrorHandler,
              meterRegistry);
      this.store =
          new DummyEphemeralCreatableUpdatableCacheableMetadataStore(
              "/snapshots", zkMetadataStore, new SnapshotMetadataSerializer(), LOG);
    }

    @After
    public void tearDown() throws IOException {
      zkMetadataStore.close();
      testingServer.close();
      meterRegistry.close();
    }

    @Test
    public void testCreateGetDelete() throws ExecutionException, InterruptedException {
      final String name = "testSnapshot";
      final String snapshotPath = "s3://snapshots/path";
      final String snapshotId = "testSnapshotId";
      final long startTimeUtc = 12345;
      final long endTimeUtc = 123456;
      final long maxOffset = 100;
      final String partitionId = "1";

      final SnapshotMetadata snapshot =
          new SnapshotMetadata(
              name, snapshotPath, snapshotId, startTimeUtc, endTimeUtc, maxOffset, partitionId);

      assertThat(store.list().get()).isEmpty();
      assertThat(store.create(snapshot).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.list().get()).containsOnly(snapshot);

      SnapshotMetadata metadata = store.get(name).get();
      assertThat(metadata.name).isEqualTo(name);
      assertThat(metadata.snapshotPath).isEqualTo(snapshotPath);
      assertThat(metadata.snapshotId).isEqualTo(snapshotId);
      assertThat(metadata.startTimeUtc).isEqualTo(startTimeUtc);
      assertThat(metadata.endTimeUtc).isEqualTo(endTimeUtc);
      assertThat(metadata.maxOffset).isEqualTo(maxOffset);
      assertThat(metadata.partitionId).isEqualTo(partitionId);

      String newSnapshotId = "newSnapshotId";
      final SnapshotMetadata newSnapshot =
          new SnapshotMetadata(
              name,
              snapshotPath,
              newSnapshotId,
              startTimeUtc + 1,
              endTimeUtc + 1,
              maxOffset + 100,
              partitionId);
      assertThat(store.update(newSnapshot).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.list().get()).containsOnly(newSnapshot);
      SnapshotMetadata newMetadata = store.get(name).get();
      assertThat(newMetadata.name).isEqualTo(name);
      assertThat(newMetadata.snapshotPath).isEqualTo(snapshotPath);
      assertThat(newMetadata.snapshotId).isEqualTo(newSnapshotId);
      assertThat(newMetadata.startTimeUtc).isEqualTo(startTimeUtc + 1);
      assertThat(newMetadata.endTimeUtc).isEqualTo(endTimeUtc + 1);
      assertThat(newMetadata.maxOffset).isEqualTo(maxOffset + 100);
      assertThat(newMetadata.partitionId).isEqualTo(partitionId);

      assertThat(store.delete(name).get()).isNull();
      assertThat(store.list().get()).isEmpty();
    }

    @Test
    public void testMultipleCreates() throws ExecutionException, InterruptedException {
      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      final String name2 = "snapshot2";
      SnapshotMetadata snapshot2 = makeSnapshot(name2);

      assertThat(store.list().get()).isEmpty();
      assertThat(store.create(snapshot1).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.create(snapshot2).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(2);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);

      SnapshotMetadata newSnapshot1 = makeSnapshot(name1, 300);
      assertThat(store.update(newSnapshot1).get()).isNull();
      assertThat(store.list().get()).containsOnly(newSnapshot1, snapshot2);

      // Adding a snapshot with the same name but different values throws exception.
      SnapshotMetadata duplicateSnapshot2 = makeSnapshot(name2, 300);
      Throwable duplicateEx = catchThrowable(() -> store.create(duplicateSnapshot2).get());
      assertThat(duplicateEx.getCause()).isInstanceOf(NodeExistsException.class);

      assertThat(store.delete(name2).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.list().get()).containsOnly(newSnapshot1);

      assertThat(store.delete(name1).get()).isNull();
      assertThat(store.list().get().isEmpty()).isTrue();

      Throwable deleteEx = catchThrowable(() -> store.delete(name1).get());
      assertThat(deleteEx.getCause()).isInstanceOf(NoNodeException.class);
    }

    @Test
    public void testDuplicateCreateNode() throws ExecutionException, InterruptedException {
      final String name = "testSnapshot";
      final String snapshotPath = "s3://snapshots/path";
      final String snapshotId = "testSnapshotId";
      final long startTimeUtc = 12345;
      final long endTimeUtc = 123456;
      final long maxOffset = 100;
      final String partitionId = "1";

      final SnapshotMetadata testSnapshot =
          new SnapshotMetadata(
              name, snapshotPath, snapshotId, startTimeUtc, endTimeUtc, maxOffset, partitionId);
      assertThat(store.create(testSnapshot).get()).isNull();

      SnapshotMetadata metadata = store.get(name).get();
      assertThat(metadata.name).isEqualTo(name);
      assertThat(metadata.snapshotPath).isEqualTo(snapshotPath);
      assertThat(metadata.snapshotId).isEqualTo(snapshotId);
      assertThat(metadata.startTimeUtc).isEqualTo(startTimeUtc);
      assertThat(metadata.endTimeUtc).isEqualTo(endTimeUtc);
      assertThat(metadata.maxOffset).isEqualTo(maxOffset);
      assertThat(metadata.partitionId).isEqualTo(partitionId);

      Throwable duplicateCreateEx = catchThrowable(() -> store.create(testSnapshot).get());
      assertThat(duplicateCreateEx.getCause()).isInstanceOf(NodeExistsException.class);

      assertThat(store.delete(name).get()).isNull();

      Throwable getMissingNodeEx = catchThrowable(() -> store.get(name).get());
      assertThat(getMissingNodeEx.getCause()).isInstanceOf(NoNodeException.class);

      Throwable deleteMissingNodeEx = catchThrowable(() -> store.delete(name).get());
      assertThat(deleteMissingNodeEx.getCause()).isInstanceOf(NoNodeException.class);
    }

    @Test
    public void testStoreOperationsOnStoppedServer()
        throws ExecutionException, InterruptedException, IOException {
      assertThat(store.list().get().isEmpty()).isTrue();

      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      assertThat(store.create(snapshot1).get()).isNull();
      assertThat(store.list().get()).containsOnly(snapshot1);
      assertThat(store.list().get().size()).isEqualTo(1);

      // Stop the ZK server
      testingServer.stop();

      Throwable createEx = catchThrowable(() -> store.create(snapshot1).get());
      assertThat(createEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

      Throwable updateEx = catchThrowable(() -> store.update(snapshot1).get());
      assertThat(updateEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

      Throwable deleteEx = catchThrowable(() -> store.delete(name1).get());
      assertThat(deleteEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

      Throwable listEx = catchThrowable(() -> store.list().get());
      assertThat(listEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

      Throwable getEx = catchThrowable(() -> store.get(name1).get());
      assertThat(getEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);
    }

    @Test
    public void testNotificationFiresOnCreate() throws ExecutionException, InterruptedException {
      assertThat(store.list().get().isEmpty()).isTrue();

      CountDownLatch notificationCountDownLatch = new CountDownLatch(1);
      AtomicInteger notificationCounter = new AtomicInteger(0);
      final KaldbMetadataStoreChangeListener testListener =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };

      store.addListener(testListener);

      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      assertThat(store.create(snapshot1).get()).isNull();
      notificationCountDownLatch.await();
      assertThat(store.getCached()).containsOnly(snapshot1);
      assertThat(store.list().get()).containsOnly(snapshot1);
      assertThat(notificationCounter.get()).isEqualTo(1);
    }

    @Test
    public void testNotificationFiresOnDataChange()
        throws ExecutionException, InterruptedException {
      assertThat(store.list().get().isEmpty()).isTrue();

      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      assertThat(store.create(snapshot1).get()).isNull();
      await().until(() -> store.getCached().size() == 1);
      assertThat(store.getCached()).containsOnly(snapshot1);
      assertThat(store.list().get()).containsOnly(snapshot1);

      CountDownLatch notificationCountDownLatch = new CountDownLatch(1);
      AtomicInteger notificationCounter = new AtomicInteger(0);

      final KaldbMetadataStoreChangeListener testListener =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };

      store.addListener(testListener);

      SnapshotMetadata newSnapshot1 = makeSnapshot(name1, 30000);

      assertThat(store.update(newSnapshot1).get()).isNull();
      notificationCountDownLatch.await();
      await().until(() -> store.getCached().contains(newSnapshot1));
      assertThat(store.getCached()).containsOnly(newSnapshot1);
      assertThat(store.list().get()).containsOnly(newSnapshot1);
      assertThat(notificationCounter.get()).isEqualTo(1);
    }

    @Test
    public void testNotificationFiresOnRemove() throws ExecutionException, InterruptedException {
      assertThat(store.list().get().isEmpty()).isTrue();

      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      assertThat(store.create(snapshot1).get()).isNull();
      await().until(() -> store.getCached().size() == 1);
      assertThat(store.getCached()).containsOnly(snapshot1);
      assertThat(store.list().get()).containsOnly(snapshot1);

      CountDownLatch notificationCountDownLatch = new CountDownLatch(1);
      AtomicInteger notificationCounter = new AtomicInteger(0);

      final KaldbMetadataStoreChangeListener testListener =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };

      store.addListener(testListener);

      assertThat(store.delete(name1).get()).isNull();
      notificationCountDownLatch.await();
      assertThat(store.getCached().isEmpty()).isTrue();
      assertThat(store.list().get().isEmpty()).isTrue();
      assertThat(notificationCounter.get()).isEqualTo(1);
    }

    @Test
    public void testMultipleWatchersOnMetadataStore()
        throws ExecutionException, InterruptedException {
      assertThat(store.list().get().isEmpty()).isTrue();

      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      assertThat(store.create(snapshot1).get()).isNull();
      await().until(() -> store.getCached().size() == 1);
      assertThat(store.getCached()).containsOnly(snapshot1);
      assertThat(store.list().get()).containsOnly(snapshot1);

      final CountDownLatch notificationCountDownLatch = new CountDownLatch(1);
      final AtomicInteger notificationCounter = new AtomicInteger(0);
      final KaldbMetadataStoreChangeListener testListener1 =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };
      store.addListener(testListener1);

      final CountDownLatch notificationCountDownLatch2 = new CountDownLatch(1);
      final KaldbMetadataStoreChangeListener testListener2 =
          () -> {
            notificationCountDownLatch2.countDown();
            notificationCounter.incrementAndGet();
          };
      store.addListener(testListener2);

      final String name2 = "snapshot2";
      SnapshotMetadata snapshot2 = makeSnapshot(name2);
      assertThat(store.create(snapshot2).get()).isNull();
      notificationCountDownLatch.await();
      notificationCountDownLatch2.await();
      assertThat(store.getCached()).containsOnly(snapshot1, snapshot2);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);
      assertThat(store.get(name2).get()).isEqualTo(snapshot2);
      assertThat(notificationCounter.get()).isEqualTo(2);
    }

    @Test
    public void testThrowingListenerOnMetadataStore()
        throws ExecutionException, InterruptedException {
      assertThat(store.list().get().isEmpty()).isTrue();

      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      assertThat(store.create(snapshot1).get()).isNull();
      await().until(() -> store.getCached().size() == 1);
      assertThat(store.getCached()).containsOnly(snapshot1);
      assertThat(store.list().get()).containsOnly(snapshot1);

      final CountDownLatch notificationCountDownLatch = new CountDownLatch(3);
      final AtomicInteger notificationCounter = new AtomicInteger(0);

      final KaldbMetadataStoreChangeListener regularListener =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };
      store.addListener(regularListener);

      final KaldbMetadataStoreChangeListener throwingListener =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
            throw new RuntimeException("test exception");
          };
      store.addListener(throwingListener);

      final KaldbMetadataStoreChangeListener regularListener2 =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };
      store.addListener(regularListener2);

      final String name2 = "snapshot2";
      SnapshotMetadata snapshot2 = makeSnapshot(name2);
      assertThat(store.create(snapshot2).get()).isNull();
      notificationCountDownLatch.await();
      assertThat(store.getCached()).containsOnly(snapshot1, snapshot2);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);
      assertThat(store.get(name2).get()).isEqualTo(snapshot2);
      assertThat(notificationCounter.get()).isEqualTo(3);
    }

    @Test
    public void testRemoveListenerOnMetadataStore()
        throws ExecutionException, InterruptedException {
      assertThat(store.list().get().isEmpty()).isTrue();

      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      assertThat(store.create(snapshot1).get()).isNull();
      await().until(() -> store.getCached().size() == 1);
      assertThat(store.getCached()).containsOnly(snapshot1);
      assertThat(store.list().get()).containsOnly(snapshot1);

      final CountDownLatch notificationCountDownLatch = new CountDownLatch(3);
      final AtomicInteger notificationCounter = new AtomicInteger(0);

      final KaldbMetadataStoreChangeListener regularListener =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };
      store.addListener(regularListener);

      final KaldbMetadataStoreChangeListener regularListener2 =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };
      store.addListener(regularListener2);

      final String name2 = "snapshot2";
      SnapshotMetadata snapshot2 = makeSnapshot(name2);
      assertThat(store.create(snapshot2).get()).isNull();
      await().until(() -> store.getCached().contains(snapshot2));
      assertThat(store.getCached()).containsOnly(snapshot1, snapshot2);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);
      assertThat(store.get(name2).get()).isEqualTo(snapshot2);
      assertThat(notificationCounter.get()).isEqualTo(2);

      // Remove listener
      store.removeListener(regularListener2);
      assertThat(store.delete(name2).get()).isNull();
      notificationCountDownLatch.await();
      assertThat(store.getCached()).containsOnly(snapshot1);
      assertThat(store.list().get()).containsOnly(snapshot1);
      assertThat(store.get(name1).get()).isEqualTo(snapshot1);
      assertThat(notificationCounter.get()).isEqualTo(3);

      Throwable getEx = catchThrowable(() -> store.get(name2).get());
      assertThat(getEx.getCause()).isInstanceOf(NoNodeException.class);
    }

    @Test
    public void testCorruptZkMetadata() throws ExecutionException, InterruptedException {
      assertThat(store.list().get().isEmpty()).isTrue();

      final CountDownLatch notificationCountDownLatch = new CountDownLatch(2);
      final AtomicInteger notificationCounter = new AtomicInteger(0);
      final KaldbMetadataStoreChangeListener regularListener =
          () -> {
            notificationCountDownLatch.countDown();
            notificationCounter.incrementAndGet();
          };
      store.addListener(regularListener);

      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      assertThat(store.create(snapshot1).get()).isNull();
      await().until(() -> store.getCached().size() == 1);
      assertThat(store.getCached()).containsOnly(snapshot1);
      assertThat(store.list().get()).containsOnly(snapshot1);

      final String name2 = "snapshot2";
      SnapshotMetadata snapshot2 = makeSnapshot(name2);
      assertThat(store.create(snapshot2).get()).isNull();
      notificationCountDownLatch.await();
      await().until(() -> store.getCached().size() == 2);
      assertThat(store.getCached()).containsOnly(snapshot1, snapshot2);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);

      // Corrupt the metadata store.
      assertThat(zkMetadataStore.put("/snapshots/" + name1, "corrupt").get()).isNull();

      // Get throws exception but store is fine.
      Throwable getEx = catchThrowable(() -> store.get(name1).get());
      assertThat(getEx.getCause()).isInstanceOf(CorruptMetadataNodeException.class);
      assertThat(store.get(name2).get()).isEqualTo(snapshot2);

      assertThat(store.getCached()).containsOnly(snapshot2);
      assertThat(store.list().get()).containsOnly(null, snapshot2);
    }
  }

  public static class TestEphemeralCreatableCacheableMetadataStore {
    private static class DummyEphemeralCreatableCacheableMetadataStore
        extends EphemeralMutableMetadataStore<SnapshotMetadata> {
      public DummyEphemeralCreatableCacheableMetadataStore(
          String storeFolder,
          MetadataStore metadataStore,
          MetadataSerializer<SnapshotMetadata> metadataSerializer,
          Logger logger)
          throws Exception {
        super(true, false, storeFolder, metadataStore, metadataSerializer, logger);
      }
    }

    private static final Logger LOG =
        LoggerFactory.getLogger(TestPersistentCreatableCacheableMetadataStore.class);

    private TestingServer testingServer;
    private ZookeeperMetadataStoreImpl zkMetadataStore;
    private MeterRegistry meterRegistry;
    private DummyEphemeralCreatableCacheableMetadataStore store;

    @Before
    public void setUp() throws Exception {
      meterRegistry = new SimpleMeterRegistry();
      // NOTE: Sometimes the ZK server fails to start. Handle it more gracefully, if tests are
      // flaky.
      testingServer = new TestingServer();
      CountingFatalErrorHandler countingFatalErrorHandler = new CountingFatalErrorHandler();
      zkMetadataStore =
          new ZookeeperMetadataStoreImpl(
              testingServer.getConnectString(),
              "test",
              1000,
              1000,
              new RetryNTimes(1, 500),
              countingFatalErrorHandler,
              meterRegistry);
      this.store =
          new DummyEphemeralCreatableCacheableMetadataStore(
              "/snapshots", zkMetadataStore, new SnapshotMetadataSerializer(), LOG);
    }

    @After
    public void tearDown() throws IOException {
      zkMetadataStore.close();
      testingServer.close();
      meterRegistry.close();
    }

    @Test
    public void testCrudOperations() throws ExecutionException, InterruptedException {
      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      final String name2 = "snapshot2";
      SnapshotMetadata snapshot2 = makeSnapshot(name2);

      assertThat(store.list().get()).isEmpty();
      assertThat(store.create(snapshot1).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.create(snapshot2).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(2);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);
      assertThat(store.getCached()).containsOnly(snapshot1, snapshot2);

      // Updates throw an exception.
      SnapshotMetadata newSnapshot1 = makeSnapshot(name1, 300);
      Throwable updateEx = catchThrowable(() -> store.update(newSnapshot1).get());
      assertThat(updateEx).isInstanceOf(UnsupportedOperationException.class);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);
      assertThat(store.getCached()).containsOnly(snapshot1, snapshot2);

      // Adding a snapshot with the same name but different values throws exception.
      SnapshotMetadata duplicateSnapshot2 = makeSnapshot(name2, 300);
      Throwable duplicateEx = catchThrowable(() -> store.create(duplicateSnapshot2).get());
      assertThat(duplicateEx.getCause()).isInstanceOf(NodeExistsException.class);

      assertThat(store.delete(name2).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.list().get()).containsOnly(snapshot1);
      assertThat(store.getCached()).containsOnly(snapshot1);

      assertThat(store.delete(name1).get()).isNull();
      assertThat(store.list().get().isEmpty()).isTrue();

      Throwable deleteEx = catchThrowable(() -> store.delete(name1).get());
      assertThat(deleteEx.getCause()).isInstanceOf(NoNodeException.class);
    }
  }

  public static class TestEphemeralCreatableMetadataStore {
    private static class DummyEphemeralCreatableMetadataStore
        extends EphemeralMutableMetadataStore<SnapshotMetadata> {
      public DummyEphemeralCreatableMetadataStore(
          String storeFolder,
          MetadataStore metadataStore,
          MetadataSerializer<SnapshotMetadata> metadataSerializer,
          Logger logger)
          throws Exception {
        super(false, false, storeFolder, metadataStore, metadataSerializer, logger);
      }
    }

    private static final Logger LOG =
        LoggerFactory.getLogger(TestPersistentCreatableCacheableMetadataStore.class);

    private TestingServer testingServer;
    private ZookeeperMetadataStoreImpl zkMetadataStore;
    private MeterRegistry meterRegistry;
    private DummyEphemeralCreatableMetadataStore store;

    @Before
    public void setUp() throws Exception {
      meterRegistry = new SimpleMeterRegistry();
      // NOTE: Sometimes the ZK server fails to start. Handle it more gracefully, if tests are
      // flaky.
      testingServer = new TestingServer();
      CountingFatalErrorHandler countingFatalErrorHandler = new CountingFatalErrorHandler();
      zkMetadataStore =
          new ZookeeperMetadataStoreImpl(
              testingServer.getConnectString(),
              "test",
              1000,
              1000,
              new RetryNTimes(1, 500),
              countingFatalErrorHandler,
              meterRegistry);
      this.store =
          new DummyEphemeralCreatableMetadataStore(
              "/snapshots", zkMetadataStore, new SnapshotMetadataSerializer(), LOG);
    }

    @After
    public void tearDown() throws IOException {
      zkMetadataStore.close();
      testingServer.close();
      meterRegistry.close();
    }

    @Test
    public void testCrudOperationsAndCache() throws ExecutionException, InterruptedException {
      final String name1 = "snapshot1";
      SnapshotMetadata snapshot1 = makeSnapshot(name1);
      final String name2 = "snapshot2";
      SnapshotMetadata snapshot2 = makeSnapshot(name2);

      assertThat(store.list().get()).isEmpty();
      assertThat(store.create(snapshot1).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.create(snapshot2).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(2);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);

      // Caching is disabled.
      Throwable getCacheEx = catchThrowable(() -> store.getCached());
      assertThat(getCacheEx).isInstanceOf(UnsupportedOperationException.class);

      // Updates throw an exception.
      SnapshotMetadata newSnapshot1 = makeSnapshot(name1, 300);
      Throwable updateEx = catchThrowable(() -> store.update(newSnapshot1).get());
      assertThat(updateEx).isInstanceOf(UnsupportedOperationException.class);
      assertThat(store.list().get()).containsOnly(snapshot1, snapshot2);

      // All cache operations are disabled.
      Throwable getCacheEx2 = catchThrowable(() -> store.getCached());
      KaldbMetadataStoreChangeListener listener = () -> {};
      assertThat(getCacheEx2).isInstanceOf(UnsupportedOperationException.class);
      Throwable addListenerEx = catchThrowable(() -> store.addListener(listener));
      assertThat(addListenerEx).isInstanceOf(UnsupportedOperationException.class);
      Throwable removeListenerEx = catchThrowable(() -> store.removeListener(listener));
      assertThat(removeListenerEx).isInstanceOf(UnsupportedOperationException.class);
      Throwable closeEx = catchThrowable(() -> store.close());
      assertThat(closeEx).isInstanceOf(UnsupportedOperationException.class);

      // Adding a snapshot with the same name but different values throws exception.
      SnapshotMetadata duplicateSnapshot2 = makeSnapshot(name2, 300);
      Throwable duplicateEx = catchThrowable(() -> store.create(duplicateSnapshot2).get());
      assertThat(duplicateEx.getCause()).isInstanceOf(NodeExistsException.class);

      assertThat(store.delete(name2).get()).isNull();
      assertThat(store.list().get().size()).isEqualTo(1);
      assertThat(store.list().get()).containsOnly(snapshot1);

      assertThat(store.delete(name1).get()).isNull();
      assertThat(store.list().get().isEmpty()).isTrue();

      Throwable deleteEx = catchThrowable(() -> store.delete(name1).get());
      assertThat(deleteEx.getCause()).isInstanceOf(NoNodeException.class);
    }
  }
}
