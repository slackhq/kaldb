package com.slack.kaldb.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.MetricsUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.JacksonModelSerializer;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KaldbMetadataStoreTest {
  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  private AsyncCuratorFramework curatorFramework;

  private KaldbConfigs.ZookeeperConfig zookeeperConfig;

  @BeforeEach
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    // NOTE: Sometimes the ZK server fails to start. Handle it more gracefully, if tests are
    // flaky.
    testingServer = new TestingServer();

    zookeeperConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("Test")
            .setZkSessionTimeoutMs(2500)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(500)
            .build();
    this.curatorFramework = CuratorBuilder.build(meterRegistry, zookeeperConfig);
  }

  @AfterEach
  public void tearDown() throws Exception {
    curatorFramework.unwrap().close();
    testingServer.close();
    meterRegistry.close();

    // clear any overrides
    System.clearProperty(KaldbMetadataStore.PERSISTENT_EPHEMERAL_PROPERTY);
  }

  private static class TestMetadata extends KaldbMetadata {
    private String value;

    @JsonCreator
    public TestMetadata(@JsonProperty("name") String name, @JsonProperty("value") String value) {
      super(name);
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TestMetadata)) return false;
      if (!super.equals(o)) return false;

      TestMetadata metadata = (TestMetadata) o;

      return value.equals(metadata.value);
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + value.hashCode();
      return result;
    }
  }

  private static final String STORE_FOLDER = "/testMetadata";

  @Test
  public void testCrudOperations() {
    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore() {
        super(
            curatorFramework,
            CreateMode.PERSISTENT,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER,
            meterRegistry);
      }
    }

    try (KaldbMetadataStore<TestMetadata> store = new TestMetadataStore()) {
      // create two metadata
      TestMetadata metadata1 = new TestMetadata("foo", "val1");
      TestMetadata metadata2 = new TestMetadata("bar", "val2");
      store.createSync(metadata1);
      store.createSync(metadata2);

      // do a non-cached list to ensure both are persisted
      List<TestMetadata> metadataListUncached = KaldbMetadataTestUtils.listSyncUncached(store);
      assertThat(metadataListUncached).containsExactlyInAnyOrder(metadata1, metadata2);

      // check to see if the cache contains the elements as well
      await().until(() -> store.listSync().containsAll(List.of(metadata1, metadata2)));

      // update the value of one of the nodes
      String updatedValue = "updatedVal1";
      metadata1.setValue(updatedValue);
      store.updateSync(metadata1);

      // check that the node was updated using both a sync get, and checking the cache as well
      await().until(() -> Objects.equals(store.getSync(metadata1.name).getValue(), updatedValue));

      await()
          .until(
              () ->
                  store.listSync().stream()
                      .filter(instance -> instance.name.equals("foo"))
                      .findFirst()
                      .get()
                      .getValue()
                      .equals(updatedValue));

      // delete a node by object reference, and ensure that list and cache both reflect the change
      store.deleteSync(metadata2);
      assertThat(KaldbMetadataTestUtils.listSyncUncached(store)).containsExactly(metadata1);
      assertThat(store.listSync()).containsExactly(metadata1);

      // delete a node by path reference, and ensure that list and cache both reflect the change
      store.deleteSync(metadata1.name);
      assertThat(KaldbMetadataTestUtils.listSyncUncached(store)).isEmpty();
      await().until(() -> store.listSync().isEmpty());
    }
  }

  @Test
  public void testDuplicateCreate() {
    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore() {
        super(
            curatorFramework,
            CreateMode.PERSISTENT,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER,
            meterRegistry);
      }
    }

    try (KaldbMetadataStore<TestMetadata> store = new TestMetadataStore()) {
      TestMetadata metadata1 = new TestMetadata("foo", "val1");
      store.createSync(metadata1);
      assertThatExceptionOfType(InternalMetadataStoreException.class)
          .isThrownBy(() -> store.createSync(metadata1));
    }
  }

  @Test
  public void testUncachedStoreAttemptingCacheOperations() {
    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore() {
        super(
            curatorFramework,
            CreateMode.PERSISTENT,
            false,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER,
            meterRegistry);
      }
    }

    try (KaldbMetadataStore<TestMetadata> store = new TestMetadataStore()) {
      // create metadata
      TestMetadata metadata1 = new TestMetadata("foo", "val1");
      store.createSync(metadata1);

      await()
          .until(
              () -> {
                List<TestMetadata> metadata = KaldbMetadataTestUtils.listSyncUncached(store);
                return metadata.contains(metadata1) && metadata.size() == 1;
              });

      // verify exceptions are thrown attempting to use cached methods
      assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(store::listSync);
      assertThatExceptionOfType(UnsupportedOperationException.class)
          .isThrownBy(() -> store.addListener((metadata) -> {}));
      assertThatExceptionOfType(UnsupportedOperationException.class)
          .isThrownBy(() -> store.removeListener((metadata) -> {}));
    }
  }

  @Test
  public void testEphemeralNodeBehavior() {
    class PersistentMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public PersistentMetadataStore() {
        super(
            curatorFramework,
            CreateMode.PERSISTENT,
            false,
            new JacksonModelSerializer<>(TestMetadata.class),
            "/persistent",
            meterRegistry);
      }
    }

    class EphemeralMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public EphemeralMetadataStore() {
        super(
            curatorFramework,
            CreateMode.EPHEMERAL,
            false,
            new JacksonModelSerializer<>(TestMetadata.class),
            "/ephemeral",
            meterRegistry);
      }
    }

    TestMetadata metadata1 = new TestMetadata("foo", "val1");
    try (KaldbMetadataStore<TestMetadata> persistentStore = new PersistentMetadataStore()) {
      // create metadata
      persistentStore.createSync(metadata1);

      // do a non-cached list to ensure node has been persisted
      assertThat(KaldbMetadataTestUtils.listSyncUncached(persistentStore))
          .containsExactly(metadata1);
    }

    TestMetadata metadata2 = new TestMetadata("foo", "val1");
    try (KaldbMetadataStore<TestMetadata> ephemeralStore = new EphemeralMetadataStore()) {
      // create metadata
      ephemeralStore.createSync(metadata2);

      // do a non-cached list to ensure node has been persisted
      assertThat(KaldbMetadataTestUtils.listSyncUncached(ephemeralStore))
          .containsExactly(metadata2);
    }

    // close curator, and then instantiate a new copy
    // This is because we cannot restart the closed curator.
    curatorFramework.unwrap().close();
    curatorFramework = CuratorBuilder.build(meterRegistry, zookeeperConfig);

    try (KaldbMetadataStore<TestMetadata> persistentStore = new PersistentMetadataStore()) {
      assertThat(persistentStore.getSync("foo")).isEqualTo(metadata1);
    }

    try (KaldbMetadataStore<TestMetadata> ephemeralStore = new EphemeralMetadataStore()) {
      assertThatExceptionOfType(InternalMetadataStoreException.class)
          .isThrownBy(() -> ephemeralStore.getSync("foo"));
    }
  }

  @Test
  public void testListenersWithZkReconnect() throws Exception {
    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore() {
        super(
            curatorFramework,
            CreateMode.PERSISTENT,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER,
            meterRegistry);
      }
    }

    try (KaldbMetadataStore<TestMetadata> store = new TestMetadataStore()) {
      AtomicInteger counter = new AtomicInteger(0);
      KaldbMetadataStoreChangeListener<TestMetadata> listener =
          (testMetadata) -> counter.incrementAndGet();
      store.addListener(listener);

      await().until(() -> counter.get() == 0);

      // create metadata
      TestMetadata metadata1 = new TestMetadata("foo", "val1");
      store.createSync(metadata1);

      await().until(() -> counter.get() == 1);

      testingServer.restart();

      assertThat(store.getSync(metadata1.name)).isEqualTo(metadata1);
      assertThat(counter.get()).isEqualTo(1);

      metadata1.setValue("val2");
      store.updateSync(metadata1);

      await().until(() -> counter.get() == 2);
    }
  }

  @Test
  public void testAddRemoveListener() throws Exception {
    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore() {
        super(
            curatorFramework,
            CreateMode.PERSISTENT,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER,
            meterRegistry);
      }
    }

    try (KaldbMetadataStore<TestMetadata> store = new TestMetadataStore()) {
      AtomicInteger counter = new AtomicInteger(0);
      KaldbMetadataStoreChangeListener<TestMetadata> listener =
          (testMetadata) -> counter.incrementAndGet();
      store.addListener(listener);

      await().until(() -> counter.get() == 0);

      // create metadata
      TestMetadata metadata1 = new TestMetadata("foo", "val1");
      store.createSync(metadata1);

      await().until(() -> counter.get() == 1);

      assertThat(store.getSync(metadata1.name)).isEqualTo(metadata1);
      assertThat(counter.get()).isEqualTo(1);

      metadata1.setValue("val2");
      store.updateSync(metadata1);

      await().until(() -> counter.get() == 2);

      store.removeListener(listener);
      metadata1.setValue("val3");
      store.updateSync(metadata1);

      Thread.sleep(2000);
      assertThat(counter.get()).isEqualTo(2);
    }
  }

  @Test
  public void serializeAndDeserializeOnlyInvokeOnce() {
    AtomicInteger serializeCounter = new AtomicInteger(0);
    AtomicInteger deserializeCounter = new AtomicInteger(0);

    class CountingSerializer implements ModelSerializer<TestMetadata> {
      final JacksonModelSerializer<TestMetadata> serializer =
          new JacksonModelSerializer<>(TestMetadata.class);

      @Override
      public byte[] serialize(TestMetadata model) {
        serializeCounter.incrementAndGet();
        return serializer.serialize(model);
      }

      @Override
      public TestMetadata deserialize(byte[] bytes) {
        deserializeCounter.incrementAndGet();
        return serializer.deserialize(bytes);
      }
    }

    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore() {
        super(
            curatorFramework,
            CreateMode.PERSISTENT,
            true,
            new CountingSerializer(),
            STORE_FOLDER,
            meterRegistry);
      }
    }

    try (KaldbMetadataStore<TestMetadata> store = new TestMetadataStore()) {
      TestMetadata metadata = new TestMetadata("name", "value");
      store.createSync(metadata);

      Thread.sleep(500);

      store.getSync("name");
      store.getSync("name");

      assertThat(serializeCounter.get()).isEqualTo(1);
      assertThat(deserializeCounter.get()).isEqualTo(1);

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testSlowCacheInitialization() {
    class FastMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public FastMetadataStore() {
        super(
            curatorFramework,
            CreateMode.PERSISTENT,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER,
            meterRegistry);
      }
    }

    class SlowSerializer implements ModelSerializer<TestMetadata> {
      final JacksonModelSerializer<TestMetadata> serializer =
          new JacksonModelSerializer<>(TestMetadata.class);

      @Override
      public byte[] serialize(TestMetadata model) {
        return serializer.serialize(model);
      }

      @Override
      public TestMetadata deserialize(byte[] bytes) {
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return serializer.deserialize(bytes);
      }
    }

    class SlowMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public SlowMetadataStore() {
        super(
            curatorFramework,
            CreateMode.PERSISTENT,
            true,
            new SlowSerializer(),
            STORE_FOLDER,
            meterRegistry);
      }
    }

    int testMetadataInitCount = 10;
    try (KaldbMetadataStore<TestMetadata> init = new FastMetadataStore()) {
      for (int i = 0; i < testMetadataInitCount; i++) {
        init.createSync(new TestMetadata("name" + i, "value" + i));
      }
    }

    try (KaldbMetadataStore<TestMetadata> init = new SlowMetadataStore()) {
      List<TestMetadata> metadata = init.listSync();
      assertThat(metadata.size()).isEqualTo(testMetadataInitCount);
    }
  }

  @Test
  public void testUncachedEphemeralNodesWithZkReconnect() throws Exception {
    System.setProperty(KaldbMetadataStore.PERSISTENT_EPHEMERAL_PROPERTY, "true");
    RetryPolicy retryPolicy = new RetryNTimes(1, 10);
    CuratorFramework curator =
        CuratorFrameworkFactory.builder()
            .connectString(zookeeperConfig.getZkConnectString())
            .namespace(zookeeperConfig.getZkPathPrefix())
            .connectionTimeoutMs(50)
            .sessionTimeoutMs(
                50) // this will be negotiated to a higher value, due to minSessionTimeout
            .retryPolicy(retryPolicy)
            .build();
    curator.start();
    AsyncCuratorFramework asyncCuratorFramework = AsyncCuratorFramework.wrap(curator);

    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore() {
        super(
            asyncCuratorFramework,
            CreateMode.EPHEMERAL,
            false,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER,
            meterRegistry);
      }
    }

    try (KaldbMetadataStore<TestMetadata> store = new TestMetadataStore()) {
      // create metadata
      TestMetadata metadata1 = new TestMetadata("foo", "val1");
      store.createSync(metadata1);
      long initialSession = curator.getZookeeperClient().getZooKeeper().getSessionId();
      AtomicReference<Stat> beforeStat = new AtomicReference<>();
      await()
          .until(
              () -> {
                beforeStat.set(store.hasAsync(metadata1.name).toCompletableFuture().get());
                return beforeStat.get() != null;
              });

      testingServer.stop();
      testingServer.restart();

      Thread.sleep(curator.getZookeeperClient().getLastNegotiatedSessionTimeoutMs() + 1000);
      assertThat(curator.getZookeeperClient().getZooKeeper().getSessionId())
          .isNotEqualTo(initialSession);

      assertThat(store.getSync(metadata1.name)).isEqualTo(metadata1);
      await()
          .until(
              () ->
                  MetricsUtil.getCount(
                          PersistentWatchedNode.PERSISTENT_NODE_RECREATED_COUNTER, meterRegistry)
                      >= 1);

      Stat afterStat = store.hasAsync(metadata1.name).toCompletableFuture().get();
      assertNotNull(beforeStat);
      assertNotNull(afterStat);
      assertThat(beforeStat.get()).isNotEqualTo(afterStat);
      metadata1.setValue("val2");
      store.updateSync(metadata1);

      store.deleteSync(metadata1.name);
    }

    curator.close();
  }

  @Test
  public void testCachedEphemeralNodesWithZkReconnect() throws Exception {
    System.setProperty(KaldbMetadataStore.PERSISTENT_EPHEMERAL_PROPERTY, "true");
    RetryPolicy retryPolicy = new RetryNTimes(1, 10);
    CuratorFramework curator =
        CuratorFrameworkFactory.builder()
            .connectString(zookeeperConfig.getZkConnectString())
            .namespace(zookeeperConfig.getZkPathPrefix())
            .connectionTimeoutMs(50)
            .sessionTimeoutMs(50)
            .retryPolicy(retryPolicy)
            .build();
    curator.start();
    AsyncCuratorFramework asyncCuratorFramework = AsyncCuratorFramework.wrap(curator);

    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore() {
        super(
            asyncCuratorFramework,
            CreateMode.EPHEMERAL,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER,
            meterRegistry);
      }
    }

    try (KaldbMetadataStore<TestMetadata> store = new TestMetadataStore()) {
      // create metadata
      TestMetadata metadata1 = new TestMetadata("foo", "val1");
      store.createSync(metadata1);
      long initialSession = curator.getZookeeperClient().getZooKeeper().getSessionId();
      AtomicReference<Stat> beforeStat = new AtomicReference<>();
      await()
          .until(
              () -> {
                beforeStat.set(store.hasAsync(metadata1.name).toCompletableFuture().get());
                return beforeStat.get() != null;
              });

      testingServer.stop();
      testingServer.restart();

      Thread.sleep(curator.getZookeeperClient().getLastNegotiatedSessionTimeoutMs() + 1000);
      assertThat(curator.getZookeeperClient().getZooKeeper().getSessionId())
          .isNotEqualTo(initialSession);

      assertThat(store.getSync(metadata1.name)).isEqualTo(metadata1);
      await()
          .until(
              () ->
                  MetricsUtil.getCount(
                          PersistentWatchedNode.PERSISTENT_NODE_RECREATED_COUNTER, meterRegistry)
                      >= 1);

      Stat afterStat = store.hasAsync(metadata1.name).toCompletableFuture().get();
      assertNotNull(beforeStat);
      assertNotNull(afterStat);
      assertThat(beforeStat.get()).isNotEqualTo(afterStat);
      metadata1.setValue("val2");
      store.updateSync(metadata1);

      store.deleteSync(metadata1.name);
    }

    curator.close();
  }

  @Test
  public void testUncachedEphemeralNodesWithZkReconnectDisabledPersistent() throws Exception {
    System.setProperty(KaldbMetadataStore.PERSISTENT_EPHEMERAL_PROPERTY, "false");
    RetryPolicy retryPolicy = new RetryNTimes(1, 10);
    CuratorFramework curator =
        CuratorFrameworkFactory.builder()
            .connectString(zookeeperConfig.getZkConnectString())
            .namespace(zookeeperConfig.getZkPathPrefix())
            .connectionTimeoutMs(50)
            .sessionTimeoutMs(50)
            .retryPolicy(retryPolicy)
            .build();
    curator.start();
    AsyncCuratorFramework asyncCuratorFramework = AsyncCuratorFramework.wrap(curator);

    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore() {
        super(
            asyncCuratorFramework,
            CreateMode.EPHEMERAL,
            false,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER,
            meterRegistry);
      }
    }

    try (KaldbMetadataStore<TestMetadata> store = new TestMetadataStore()) {
      // create metadata
      TestMetadata metadata1 = new TestMetadata("foo", "val1");
      store.createSync(metadata1);
      long initialSession = curator.getZookeeperClient().getZooKeeper().getSessionId();

      testingServer.stop();
      testingServer.restart();

      Thread.sleep(curator.getZookeeperClient().getLastNegotiatedSessionTimeoutMs() + 1000);
      assertThat(curator.getZookeeperClient().getZooKeeper().getSessionId())
          .isNotEqualTo(initialSession);

      assertThrows(InternalMetadataStoreException.class, () -> store.getSync(metadata1.name));
      assertThat(
              MetricsUtil.getCount(
                  PersistentWatchedNode.PERSISTENT_NODE_RECREATED_COUNTER, meterRegistry))
          .isEqualTo(0);
    }

    curator.close();
  }

  @Test
  public void testCachedEphemeralNodesWithZkReconnectDisabledPersistent() throws Exception {
    System.setProperty(KaldbMetadataStore.PERSISTENT_EPHEMERAL_PROPERTY, "false");
    RetryPolicy retryPolicy = new RetryNTimes(1, 10);
    CuratorFramework curator =
        CuratorFrameworkFactory.builder()
            .connectString(zookeeperConfig.getZkConnectString())
            .namespace(zookeeperConfig.getZkPathPrefix())
            .connectionTimeoutMs(50)
            .sessionTimeoutMs(50)
            .retryPolicy(retryPolicy)
            .build();
    curator.start();
    AsyncCuratorFramework asyncCuratorFramework = AsyncCuratorFramework.wrap(curator);

    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore() {
        super(
            asyncCuratorFramework,
            CreateMode.EPHEMERAL,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER,
            meterRegistry);
      }
    }

    try (KaldbMetadataStore<TestMetadata> store = new TestMetadataStore()) {
      // create metadata
      TestMetadata metadata1 = new TestMetadata("foo", "val1");
      store.createSync(metadata1);
      long initialSession = curator.getZookeeperClient().getZooKeeper().getSessionId();

      testingServer.stop();
      testingServer.restart();

      Thread.sleep(curator.getZookeeperClient().getLastNegotiatedSessionTimeoutMs() + 1000);
      assertThat(curator.getZookeeperClient().getZooKeeper().getSessionId())
          .isNotEqualTo(initialSession);

      assertThrows(InternalMetadataStoreException.class, () -> store.getSync(metadata1.name));
      assertThat(
              MetricsUtil.getCount(
                  PersistentWatchedNode.PERSISTENT_NODE_RECREATED_COUNTER, meterRegistry))
          .isEqualTo(0);
    }

    curator.close();
  }

  @Test
  public void testWhenPersistentEphemeralIsUpdatedByAnotherCurator() {
    System.setProperty(KaldbMetadataStore.PERSISTENT_EPHEMERAL_PROPERTY, "true");
    RetryPolicy retryPolicy = new RetryNTimes(1, 10);
    CuratorFramework curator1 =
        CuratorFrameworkFactory.builder()
            .connectString(zookeeperConfig.getZkConnectString())
            .namespace(zookeeperConfig.getZkPathPrefix())
            .connectionTimeoutMs(50)
            .sessionTimeoutMs(50)
            .retryPolicy(retryPolicy)
            .build();
    curator1.start();
    AsyncCuratorFramework asyncCuratorFramework1 = AsyncCuratorFramework.wrap(curator1);

    CuratorFramework curator2 =
        CuratorFrameworkFactory.builder()
            .connectString(zookeeperConfig.getZkConnectString())
            .namespace(zookeeperConfig.getZkPathPrefix())
            .connectionTimeoutMs(50)
            .sessionTimeoutMs(50)
            .retryPolicy(retryPolicy)
            .build();
    curator2.start();
    AsyncCuratorFramework asyncCuratorFramework2 = AsyncCuratorFramework.wrap(curator2);

    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore(AsyncCuratorFramework curator) {
        super(
            curator,
            CreateMode.EPHEMERAL,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER,
            meterRegistry);
      }
    }

    TestMetadata metadata1 = new TestMetadata("foo", "val1");
    try (KaldbMetadataStore<TestMetadata> store1 = new TestMetadataStore(asyncCuratorFramework1)) {
      store1.createSync(metadata1);

      try (KaldbMetadataStore<TestMetadata> store2 =
          new TestMetadataStore(asyncCuratorFramework2)) {
        // curator2 updates the value
        TestMetadata metadata2 = store2.getSync(metadata1.name);
        metadata2.value = "val2";
        store2.updateSync(metadata2);
      }

      // curator1 should pickup the new update
      await().until(() -> store1.getSync(metadata1.name).getValue().equals("val2"));
      assertThat(store1.hasSync(metadata1.name)).isTrue();
    }

    curator2.close();
    curator1.close();
  }

  @Test
  public void testDoubleCreateEphemeral() {
    System.setProperty(KaldbMetadataStore.PERSISTENT_EPHEMERAL_PROPERTY, "true");
    RetryPolicy retryPolicy = new RetryNTimes(1, 10);
    CuratorFramework curator1 =
        CuratorFrameworkFactory.builder()
            .connectString(zookeeperConfig.getZkConnectString())
            .namespace(zookeeperConfig.getZkPathPrefix())
            .connectionTimeoutMs(50)
            .sessionTimeoutMs(50)
            .retryPolicy(retryPolicy)
            .build();
    curator1.start();
    AsyncCuratorFramework asyncCuratorFramework1 = AsyncCuratorFramework.wrap(curator1);

    CuratorFramework curator2 =
        CuratorFrameworkFactory.builder()
            .connectString(zookeeperConfig.getZkConnectString())
            .namespace(zookeeperConfig.getZkPathPrefix())
            .connectionTimeoutMs(50)
            .sessionTimeoutMs(50)
            .retryPolicy(retryPolicy)
            .build();
    curator2.start();
    AsyncCuratorFramework asyncCuratorFramework2 = AsyncCuratorFramework.wrap(curator2);

    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore(AsyncCuratorFramework curator) {
        super(
            curator,
            CreateMode.EPHEMERAL,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER,
            meterRegistry);
      }
    }
    TestMetadata testMetadata = new TestMetadata("foo", "vbr");
    try (KaldbMetadataStore<TestMetadata> store1 = new TestMetadataStore(asyncCuratorFramework1)) {
      store1.createSync(testMetadata);
      assertThrows(InternalMetadataStoreException.class, () -> store1.createSync(testMetadata));

      try (KaldbMetadataStore<TestMetadata> store2 =
          new TestMetadataStore(asyncCuratorFramework2)) {
        assertThrows(InternalMetadataStoreException.class, () -> store2.createSync(testMetadata));
      }
    }

    curator2.close();
    curator1.close();
  }

  @Test
  public void testWhenPersistentEphemeralIsUpdatedByAnotherCuratorWhileAway() throws Exception {
    System.setProperty(KaldbMetadataStore.PERSISTENT_EPHEMERAL_PROPERTY, "true");
    RetryPolicy retryPolicy = new RetryNTimes(1, 10);
    CuratorFramework curator1 =
        CuratorFrameworkFactory.builder()
            .connectString(zookeeperConfig.getZkConnectString())
            .namespace(zookeeperConfig.getZkPathPrefix())
            .connectionTimeoutMs(50)
            .sessionTimeoutMs(50)
            .retryPolicy(retryPolicy)
            .build();
    curator1.start();
    AsyncCuratorFramework asyncCuratorFramework1 = AsyncCuratorFramework.wrap(curator1);

    CuratorFramework curator2 =
        CuratorFrameworkFactory.builder()
            .connectString(zookeeperConfig.getZkConnectString())
            .namespace(zookeeperConfig.getZkPathPrefix())
            .connectionTimeoutMs(50)
            .sessionTimeoutMs(50)
            .retryPolicy(retryPolicy)
            .build();
    curator2.start();
    AsyncCuratorFramework asyncCuratorFramework2 = AsyncCuratorFramework.wrap(curator2);

    class TestMetadataStore extends KaldbMetadataStore<TestMetadata> {
      public TestMetadataStore(AsyncCuratorFramework curator) {
        super(
            curator,
            CreateMode.EPHEMERAL,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            STORE_FOLDER,
            meterRegistry);
      }
    }

    TestMetadata metadata1 = new TestMetadata("foo", "val1");
    try (KaldbMetadataStore<TestMetadata> store1 = new TestMetadataStore(asyncCuratorFramework1)) {
      store1.createSync(metadata1);

      long sessionId1 = curator1.getZookeeperClient().getZooKeeper().getSessionId();
      try (KaldbMetadataStore<TestMetadata> store2 =
          new TestMetadataStore(asyncCuratorFramework2)) {
        // curator2 updates the value
        TestMetadata metadata2 = store2.getSync(metadata1.name);
        metadata2.value = "val2";

        // force a ZK session close
        curator1.getZookeeperClient().getZooKeeper().close();
        assertThrows(InternalMetadataStoreException.class, () -> store2.updateSync(metadata2));
      }

      await()
          .atMost(30, TimeUnit.SECONDS)
          .until(() -> curator1.getZookeeperClient().getZooKeeper().getSessionId() != sessionId1);

      // wait until the node shows back up on ZK
      await().until(() -> store1.hasSync(metadata1.name));
      assertThat(store1.getSync(metadata1.name).getValue()).isEqualTo("val1");
    }

    curator2.close();
    curator1.close();
  }
}
