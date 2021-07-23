package com.slack.kaldb.metadata.zookeeper;

import static com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl.METADATA_FAILED_COUNTER;
import static com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl.METADATA_READ_COUNTER;
import static com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl.METADATA_WRITE_COUNTER;
import static com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl.ZK_FAILED_COUNTER;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.util.SnapshotUtil.makeSnapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataSerializer;
import com.slack.kaldb.util.CountingFatalErrorHandler;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZookeeperMetadataStoreImplTest {
  private TestingServer testingServer;
  private ZookeeperMetadataStoreImpl metadataStore;
  private MeterRegistry meterRegistry;
  private CountingFatalErrorHandler countingFatalErrorHandler;

  @Before
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    // NOTE: Sometimes the ZK server fails to start. Handle it more gracefully, if tests are flaky.
    testingServer = new TestingServer();
    countingFatalErrorHandler = new CountingFatalErrorHandler();
    metadataStore =
        new ZookeeperMetadataStoreImpl(
            testingServer.getConnectString(),
            "test",
            1000,
            1000,
            new RetryNTimes(1, 500),
            countingFatalErrorHandler,
            meterRegistry);
  }

  @After
  public void tearDown() throws IOException {
    metadataStore.close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void testCreateAndGetWithoutParents() throws ExecutionException, InterruptedException {
    final String rootNode = "/rootNode";
    final String rootNodeData = "rootNodeData";
    final String childNode = "/childNode1";
    final String childNodeData = "childNodeData";
    final String childNode2 = "/childNode2";
    final String childNodeData2 = "";

    assertThat(getCount(METADATA_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(metadataStore.create(rootNode, rootNodeData, false).get()).isNull();
    assertThat(metadataStore.create(rootNode + childNode, childNodeData, false).get()).isNull();
    assertThat(getCount(METADATA_WRITE_COUNTER, meterRegistry)).isEqualTo(2);
    assertThat(metadataStore.create("/rootNode/childNode2", childNodeData2, false).get()).isNull();
    assertThat(getCount(METADATA_WRITE_COUNTER, meterRegistry)).isEqualTo(3);
    assertThat(metadataStore.get(rootNode).get()).isEqualTo(rootNodeData);
    assertThat(getCount(METADATA_READ_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(metadataStore.get(rootNode + childNode).get()).isEqualTo(childNodeData);
    assertThat(metadataStore.get("/rootNode/childNode1").get()).isEqualTo(childNodeData);
    assertThat(metadataStore.get(rootNode + childNode2).get()).isEqualTo(childNodeData2);

    assertThat(getCount(METADATA_WRITE_COUNTER, meterRegistry)).isEqualTo(3);
    assertThat(getCount(METADATA_READ_COUNTER, meterRegistry)).isEqualTo(4);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(METADATA_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
  }

  @Test
  public void testGetMissingNodeThrowsException() throws ExecutionException, InterruptedException {
    Throwable throwable1 = catchThrowable(() -> metadataStore.get("/missing").get());
    assertThat(throwable1.getCause()).isInstanceOf(NoNodeException.class);

    metadataStore.create("/rootNode", "", true).get();
    metadataStore.create("/rootNode/child", "", true).get();
    assertThat(metadataStore.get("/rootNode").get()).isEqualTo("");
    assertThat(metadataStore.get("/rootNode/child").get()).isEqualTo("");

    Throwable throwable2 = catchThrowable(() -> metadataStore.get("/rootNode/missing").get());
    assertThat(throwable2.getCause()).isInstanceOf(NoNodeException.class);

    Throwable throwable3 =
        catchThrowable(() -> metadataStore.create("/root/c1/c2", "", false).get());
    assertThat(throwable3.getCause()).isInstanceOf(InternalMetadataStoreException.class);

    assertThat(getCount(METADATA_WRITE_COUNTER, meterRegistry)).isEqualTo(3);
    assertThat(getCount(METADATA_READ_COUNTER, meterRegistry)).isEqualTo(4);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(getCount(METADATA_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
  }

  @Test
  public void testCreateAndGetWithParents() throws ExecutionException, InterruptedException {
    assertThat(metadataStore.create("/root/1/2/3", "123", true).get()).isNull();
    assertThat(metadataStore.create("/root/1/4", "14", true).get()).isNull();
    assertThat(metadataStore.create("/root/2/4", "24", true).get()).isNull();
    assertThat(metadataStore.exists("/root/1").get()).isTrue();
    assertThat(metadataStore.exists("/root/1/2/3").get()).isTrue();
    assertThat(metadataStore.exists("/root/2").get()).isTrue();
    assertThat(metadataStore.exists("/root/2/4").get()).isTrue();
    assertThat(metadataStore.get("/root/1").get()).isEmpty();
    assertThat(metadataStore.get("/root/1/2").get()).isEmpty();
    assertThat(metadataStore.get("/root/2").get()).isEmpty();
    assertThat(metadataStore.get("/root/1/2/3").get()).isEqualTo("123");
    assertThat(metadataStore.get("/root/1/4").get()).isEqualTo("14");
    assertThat(metadataStore.get("/root/2/4").get()).isEqualTo("24");

    assertThat(getCount(METADATA_WRITE_COUNTER, meterRegistry)).isEqualTo(3);
    assertThat(getCount(METADATA_READ_COUNTER, meterRegistry)).isEqualTo(10);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(METADATA_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
  }

  @Test
  public void testCreatingExistingNodeThrowsException()
      throws ExecutionException, InterruptedException {
    assertThat(metadataStore.create("/root/1/2/3", "123", true).get()).isNull();
    assertThat(metadataStore.exists("/root/1/2/3").get()).isTrue();

    Throwable throwable1 =
        catchThrowable(() -> metadataStore.create("/root/1/2/3", "", true).get());
    assertThat(throwable1.getCause()).isInstanceOf(NodeExistsException.class);

    Throwable throwable2 = catchThrowable(() -> metadataStore.create("/root/1/2", "", true).get());
    assertThat(throwable2.getCause()).isInstanceOf(NodeExistsException.class);

    Throwable throwable3 = catchThrowable(() -> metadataStore.create("/root/1/2", "", false).get());
    assertThat(throwable3.getCause()).isInstanceOf(NodeExistsException.class);

    // With different data
    Throwable throwable4 =
        catchThrowable(() -> metadataStore.create("/root/1/2", "s", false).get());
    assertThat(throwable4.getCause()).isInstanceOf(NodeExistsException.class);

    // Creating a folder fails with an internal error
    Throwable throwable5 =
        catchThrowable(() -> metadataStore.create("/createAFolder/", "", false).get());
    assertThat(throwable5.getCause()).isInstanceOf(InternalMetadataStoreException.class);

    assertThat(getCount(METADATA_WRITE_COUNTER, meterRegistry)).isEqualTo(6);
    assertThat(getCount(METADATA_READ_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(METADATA_FAILED_COUNTER, meterRegistry)).isEqualTo(1);
  }

  @Test
  public void testExists() throws ExecutionException, InterruptedException {
    assertThat(metadataStore.create("/root/1/2/3", "123", true).get()).isNull();
    assertThat(metadataStore.exists("/root/1/2/3").get()).isTrue();
    assertThat(metadataStore.exists("/root/1").get()).isTrue();
    assertThat(metadataStore.exists("/root/2").get()).isFalse();
    assertThat(metadataStore.exists("/root/1/2/3/4").get()).isFalse();

    assertThat(getCount(METADATA_WRITE_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(getCount(METADATA_READ_COUNTER, meterRegistry)).isEqualTo(4);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(METADATA_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
  }

  @Test
  public void testPutOperation() throws ExecutionException, InterruptedException {
    String path = "/root/1/2/3";
    String data = "Data123";
    assertThat(metadataStore.create(path, data, true).get()).isNull();
    assertThat(metadataStore.get(path).get()).isEqualTo(data);
    String updatedNodeData = "newData123";
    assertThat(metadataStore.put(path, updatedNodeData).get()).isNull();
    assertThat(metadataStore.get(path).get()).isEqualTo(updatedNodeData);

    Throwable throwable1 = catchThrowable(() -> metadataStore.put("/missing", "m").get());
    assertThat(throwable1.getCause()).isInstanceOf(NoNodeException.class);

    assertThat(getCount(METADATA_WRITE_COUNTER, meterRegistry)).isEqualTo(3);
    assertThat(getCount(METADATA_READ_COUNTER, meterRegistry)).isEqualTo(2);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(METADATA_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
  }

  @Test
  public void testDeleteOperation() throws ExecutionException, InterruptedException {
    Throwable throwable1 = catchThrowable(() -> metadataStore.delete("/missing").get());
    assertThat(throwable1.getCause()).isInstanceOf(NoNodeException.class);

    String path1 = "/root/1/2/3";
    String path2 = "/root/1/2/4";
    String path3 = "/root/1/2/5";
    assertThat(metadataStore.create(path1, "", true).get()).isNull();
    assertThat(metadataStore.create(path2, "", true).get()).isNull();
    assertThat(metadataStore.create(path3, "", true).get()).isNull();
    assertThat(metadataStore.get(path1).get()).isEmpty();
    assertThat(metadataStore.get(path2).get()).isEmpty();
    assertThat(metadataStore.get(path3).get()).isEmpty();
    assertThat(metadataStore.delete(path1).get()).isNull();
    assertThat(metadataStore.exists(path1).get()).isFalse();
    assertThat(metadataStore.get(path2).get()).isEmpty();
    assertThat(metadataStore.get(path3).get()).isEmpty();

    Throwable nestedChildrenEx = catchThrowable(() -> metadataStore.delete("/root/1").get());
    assertThat(nestedChildrenEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

    Throwable nodeWithChildrenEx = catchThrowable(() -> metadataStore.delete("/root/1/2").get());
    assertThat(nodeWithChildrenEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

    assertThat(metadataStore.delete(path2).get()).isNull();
    assertThat(metadataStore.delete(path3).get()).isNull();
    assertThat(metadataStore.exists(path1).get()).isFalse();
    assertThat(metadataStore.exists(path2).get()).isFalse();
    assertThat(metadataStore.exists(path3).get()).isFalse();
    assertThat(metadataStore.exists("/root/1/2").get()).isTrue();
    assertThat(metadataStore.delete("/root/1/2").get()).isNull();
    assertThat(metadataStore.exists("/root/1/2").get()).isFalse();
    assertThat(metadataStore.exists("/root/1").get()).isTrue();
    assertThat(metadataStore.delete("/root/1").get()).isNull();
    assertThat(metadataStore.exists("/root/1").get()).isFalse();

    assertThat(getCount(METADATA_WRITE_COUNTER, meterRegistry)).isEqualTo(11);
    assertThat(getCount(METADATA_READ_COUNTER, meterRegistry)).isEqualTo(13);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(2);
    assertThat(getCount(METADATA_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
  }

  @Test
  public void testDeleteMissingNode() throws ExecutionException, InterruptedException {
    Throwable throwable = catchThrowable(() -> metadataStore.delete("/missing").get());
    assertThat(throwable.getCause()).isInstanceOf(NoNodeException.class);

    assertThat(metadataStore.create("/root", "", true).get()).isNull();
    assertThat(metadataStore.exists("/root").get()).isTrue();

    Throwable throwable2 = catchThrowable(() -> metadataStore.delete("/root/missing").get());
    assertThat(throwable2.getCause()).isInstanceOf(NoNodeException.class);

    assertThat(getCount(METADATA_WRITE_COUNTER, meterRegistry)).isEqualTo(3);
    assertThat(getCount(METADATA_READ_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(METADATA_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
  }

  @Test
  public void testGetChildrenSimple() throws ExecutionException, InterruptedException {
    Throwable throwable1 = catchThrowable(() -> metadataStore.getChildren("/root").get());
    assertThat(throwable1.getCause()).isInstanceOf(NoNodeException.class);

    assertThat(metadataStore.create("/root", "", true).get()).isNull();
    assertThat(metadataStore.getChildren("/root").get().size()).isZero();

    Throwable throwable2 = catchThrowable(() -> metadataStore.getChildren("/root/1").get());
    assertThat(throwable2.getCause()).isInstanceOf(NoNodeException.class);

    assertThat(metadataStore.create("/root/1", "test", true).get()).isNull();
    assertThat(metadataStore.getChildren("/root").get()).hasSameElementsAs(List.of("1"));
    assertThat(metadataStore.create("/root/2", "test", true).get()).isNull();
    assertThat(metadataStore.getChildren("/root").get()).hasSameElementsAs(List.of("1", "2"));
    assertThat(metadataStore.delete("/root/2").get()).isNull();
    assertThat(metadataStore.exists("/root/2").get()).isFalse();
    assertThat(metadataStore.getChildren("/root").get()).hasSameElementsAs(List.of("1"));

    // Invalid path
    Throwable noChildEx = catchThrowable(() -> metadataStore.getChildren("root/21").get());
    assertThat(noChildEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

    assertThat(getCount(METADATA_WRITE_COUNTER, meterRegistry)).isEqualTo(4);
    assertThat(getCount(METADATA_READ_COUNTER, meterRegistry)).isEqualTo(8);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(METADATA_FAILED_COUNTER, meterRegistry)).isEqualTo(1);
  }

  @Test
  public void testGetChildrenNested() throws ExecutionException, InterruptedException {
    String path1 = "/root/1/2/3";
    String path2 = "/root/1/2/4";
    String path3 = "/root/1/2/5";

    Throwable beforeNodeCreationEx = catchThrowable(() -> metadataStore.getChildren(path1).get());
    assertThat(beforeNodeCreationEx.getCause()).isInstanceOf(NoNodeException.class);

    assertThat(metadataStore.create(path1, "", true).get()).isNull();
    assertThat(metadataStore.create(path2, "", true).get()).isNull();
    assertThat(metadataStore.create(path3, "", true).get()).isNull();
    assertThat(metadataStore.get(path1).get()).isEmpty();
    assertThat(metadataStore.get(path2).get()).isEmpty();
    assertThat(metadataStore.get(path3).get()).isEmpty();

    List<String> rootChildren = metadataStore.getChildren("/root").get();
    assertThat(rootChildren.size()).isEqualTo(1);
    assertThat(rootChildren).hasSameElementsAs(List.of("1"));

    List<String> root1Children = metadataStore.getChildren("/root/1").get();
    assertThat(root1Children.size()).isEqualTo(1);
    assertThat(root1Children).hasSameElementsAs(List.of("2"));

    List<String> root12Children = metadataStore.getChildren("/root/1/2").get();
    assertThat(root12Children.size()).isEqualTo(3);
    assertThat(root12Children).hasSameElementsAs(List.of("3", "4", "5"));

    String path4 = "/root/1/21";
    assertThat(metadataStore.create(path4, "", true).get()).isNull();
    assertThat(metadataStore.get(path4).get()).isEmpty();
    assertThat(metadataStore.getChildren("/root").get()).hasSameElementsAs(List.of("1"));
    assertThat(metadataStore.getChildren("/root/1").get()).hasSameElementsAs(List.of("21", "2"));
    assertThat(metadataStore.getChildren("/root/1/2").get())
        .hasSameElementsAs(List.of("3", "4", "5"));

    assertThat(metadataStore.getChildren("/root/1/21").get().size()).isZero();

    // Invalid path
    Throwable noChildEx = catchThrowable(() -> metadataStore.getChildren("root/1/21").get());
    assertThat(noChildEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

    assertThat(metadataStore.delete(path3).get()).isNull();
    assertThat(metadataStore.getChildren("/root").get()).hasSameElementsAs(List.of("1"));
    assertThat(metadataStore.getChildren("/root/1").get()).hasSameElementsAs(List.of("21", "2"));
    assertThat(metadataStore.getChildren("/root/1/2").get()).hasSameElementsAs(List.of("3", "4"));

    assertThat(getCount(METADATA_WRITE_COUNTER, meterRegistry)).isEqualTo(5);
    assertThat(getCount(METADATA_READ_COUNTER, meterRegistry)).isEqualTo(16);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(METADATA_FAILED_COUNTER, meterRegistry)).isEqualTo(1);
  }

  @Test
  public void testEphemeralNodeCreateRoot() throws ExecutionException, InterruptedException {
    final String root = "/ephemeralRoot";
    final String ephemeralData = "ephemeralData";
    assertThat(metadataStore.createEphemeralNode(root, ephemeralData).get()).isNull();
    assertThat(metadataStore.exists(root).get()).isTrue();

    Throwable noEphemeralChildEx =
        catchThrowable(() -> metadataStore.createEphemeralNode(root + "/child", "").get());
    assertThat(noEphemeralChildEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

    assertThat(metadataStore.get(root).get()).isEqualTo(ephemeralData);
    assertThat(metadataStore.put(root, ephemeralData + "1").get()).isNull();
    assertThat(metadataStore.get(root).get()).isEqualTo(ephemeralData + "1");
    assertThat(metadataStore.getChildren(root).get().size()).isZero();

    Throwable duplicateEphemeralEx =
        catchThrowable(() -> metadataStore.createEphemeralNode(root, "").get());
    assertThat(duplicateEphemeralEx.getCause()).isInstanceOf(NodeExistsException.class);

    assertThat(metadataStore.delete(root).get()).isNull();
    assertThat(metadataStore.exists(root).get()).isFalse();

    assertThat(getCount(METADATA_WRITE_COUNTER, meterRegistry)).isEqualTo(5);
    assertThat(getCount(METADATA_READ_COUNTER, meterRegistry)).isEqualTo(5);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(getCount(METADATA_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
  }

  @Test
  public void testEphemeralNodeNested() throws ExecutionException, InterruptedException {
    final String root = "/root/1/2/3";
    final String persistentChild = root + "/persistent1";
    final String ephemeralNode = root + "/ephemeral";
    final String ephemeralNodeData = "ephemeral";
    assertThat(metadataStore.create(root, "", true).get()).isNull();
    assertThat(metadataStore.createEphemeralNode(ephemeralNode, ephemeralNodeData).get()).isNull();
    assertThat(metadataStore.get(ephemeralNode).get()).isEqualTo(ephemeralNodeData);
    assertThat(metadataStore.create(persistentChild, "", true).get()).isNull();
    assertThat(metadataStore.getChildren("/root/1").get()).hasSameElementsAs(List.of("2"));
    assertThat(metadataStore.getChildren(root).get())
        .hasSameElementsAs(List.of("ephemeral", "persistent1"));
    assertThat(metadataStore.exists(persistentChild).get()).isTrue();
    assertThat(metadataStore.exists(ephemeralNode).get()).isTrue();

    Throwable noEphemeralChildEx =
        catchThrowable(() -> metadataStore.createEphemeralNode(ephemeralNode + "/child", "").get());
    assertThat(noEphemeralChildEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);

    final String ephemeralNode2 = persistentChild + "/ephemeralChild2";
    assertThat(metadataStore.createEphemeralNode(ephemeralNode2, ephemeralNodeData).get()).isNull();
    assertThat(metadataStore.getChildren(persistentChild).get())
        .hasSameElementsAs(List.of("ephemeralChild2"));

    Throwable deleteWithEphemeralChildEx =
        catchThrowable(() -> metadataStore.delete(persistentChild).get());
    assertThat(deleteWithEphemeralChildEx.getCause())
        .isInstanceOf(InternalMetadataStoreException.class);

    assertThat(metadataStore.delete(ephemeralNode2).get()).isNull();
    assertThat(metadataStore.exists(ephemeralNode2).get()).isFalse();
    assertThat(metadataStore.exists(persistentChild).get()).isTrue();
    assertThat(metadataStore.exists(ephemeralNode).get()).isTrue();

    assertThat(metadataStore.delete(persistentChild).get()).isNull();
    assertThat(metadataStore.exists(persistentChild).get()).isFalse();
    assertThat(metadataStore.exists(ephemeralNode).get()).isTrue();

    Throwable duplicateEphemeralEx =
        catchThrowable(() -> metadataStore.createEphemeralNode(ephemeralNode, "").get());
    assertThat(duplicateEphemeralEx.getCause()).isInstanceOf(NodeExistsException.class);

    assertThat(metadataStore.delete(ephemeralNode).get()).isNull();
    assertThat(metadataStore.exists(ephemeralNode).get()).isFalse();
    assertThat(metadataStore.delete(root).get()).isNull();
    assertThat(metadataStore.exists(root).get()).isFalse();

    assertThat(getCount(METADATA_WRITE_COUNTER, meterRegistry)).isEqualTo(11);
    assertThat(getCount(METADATA_READ_COUNTER, meterRegistry)).isEqualTo(13);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(2);
    assertThat(getCount(METADATA_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
  }

  @Test
  public void testEphemeralNodeWithClientRestart() throws ExecutionException, InterruptedException {
    final String ephemeralRoot = "/ephemeralRoot";
    final String ephemeralData = "ephemeralData";
    assertThat(metadataStore.createEphemeralNode(ephemeralRoot, ephemeralData).get()).isNull();
    assertThat(metadataStore.exists(ephemeralRoot).get()).isTrue();
    assertThat(metadataStore.get(ephemeralRoot).get()).isEqualTo(ephemeralData);

    // Close and re-open the metadata store.
    metadataStore.close();
    metadataStore =
        new ZookeeperMetadataStoreImpl(
            testingServer.getConnectString(),
            "test",
            1000,
            1000,
            new RetryNTimes(1, 500),
            new CountingFatalErrorHandler(),
            meterRegistry);

    assertThat(metadataStore.exists(ephemeralRoot).get()).isFalse();

    assertThat(getCount(METADATA_WRITE_COUNTER, meterRegistry)).isEqualTo(1);
    assertThat(getCount(METADATA_READ_COUNTER, meterRegistry)).isEqualTo(3);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
    assertThat(getCount(METADATA_FAILED_COUNTER, meterRegistry)).isEqualTo(0);
  }

  @Test
  public void testCreateZkServerFailure() throws Exception {
    String root = "/root/1/2/3";
    assertThat(metadataStore.create(root, "", true).get()).isNull();
    assertThat(metadataStore.exists(root).get()).isTrue();
    assertThat(metadataStore.get(root).get()).isEmpty();

    // Stop ZK server to simulate ZK failure.
    testingServer.close();

    Throwable createEx = catchThrowable(() -> metadataStore.create(root, "", true).get());
    assertThat(createEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(1);

    Throwable getEx = catchThrowable(() -> metadataStore.get(root).get());
    assertThat(getEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(2);

    Throwable existsEx = catchThrowable(() -> metadataStore.exists(root).get());
    assertThat(existsEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(3);

    Throwable putEx = catchThrowable(() -> metadataStore.put(root, "newtest").get());
    assertThat(putEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(4);

    Throwable getChildrenEx = catchThrowable(() -> metadataStore.getChildren(root).get());
    assertThat(getChildrenEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(5);

    Throwable deleteEx = catchThrowable(() -> metadataStore.delete(root).get());
    assertThat(deleteEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(6);

    Throwable ephemeralEx = catchThrowable(() -> metadataStore.createEphemeralNode(root, "").get());
    assertThat(ephemeralEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(7);

    Throwable cacheCreationEx =
        catchThrowable(
            () -> metadataStore.cacheNodeAndChildren(root, new SnapshotMetadataSerializer()));
    assertThat(cacheCreationEx).isInstanceOf(InternalMetadataStoreException.class);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(8);
  }

  @Test
  public void testFatalErrorHandlerInvocation() throws Exception {
    String root = "/root/1/2/3";
    assertThat(metadataStore.create(root, "", true).get()).isNull();
    assertThat(metadataStore.exists(root).get()).isTrue();
    assertThat(metadataStore.get(root).get()).isEmpty();

    // Stop ZK server to simulate ZK failure.
    testingServer.close();

    Throwable createEx = catchThrowable(() -> metadataStore.create(root, "", true).get());
    assertThat(createEx.getCause()).isInstanceOf(InternalMetadataStoreException.class);
    assertThat(getCount(ZK_FAILED_COUNTER, meterRegistry)).isEqualTo(1);

    // The FatalErrorHandler is incremented async in a separate thread
    await().until(() -> countingFatalErrorHandler.getCount() == 1);
  }

  @Test(expected = NoNodeException.class)
  public void testCacheNodeAndChildrenCreatesMissingPath() throws Exception {
    String root = "/root";
    metadataStore.cacheNodeAndChildren(root, new SnapshotMetadataSerializer());
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void testCacheNodeAndChildren() throws Exception {
    String root = "/root";
    assertThat(metadataStore.create(root, "", true).get()).isNull();

    SnapshotMetadataSerializer serDe = new SnapshotMetadataSerializer();
    CachedMetadataStore<SnapshotMetadata> cache = metadataStore.cacheNodeAndChildren(root, serDe);
    cache.start();

    final String ephemeralNode = "/root/enode";
    SnapshotMetadata snapshot1 = makeSnapshot("test1");
    assertThat(metadataStore.createEphemeralNode(ephemeralNode, serDe.toJsonStr(snapshot1)).get())
        .isNull();

    final String persistentNode = "/root/node";
    SnapshotMetadata snapshot2 = makeSnapshot("test2");
    assertThat(metadataStore.create(persistentNode, serDe.toJsonStr(snapshot2), true).get())
        .isNull();

    await().untilAsserted(() -> assertThat(cache.getInstances().size()).isEqualTo(2));
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot2);
    assertThat(metadataStore.get(ephemeralNode).get()).isEqualTo(serDe.toJsonStr(snapshot1));
    assertThat(metadataStore.get(persistentNode).get()).isEqualTo(serDe.toJsonStr(snapshot2));
    assertThat(cache.getInstances()).containsOnly(snapshot1, snapshot2);

    SnapshotMetadata snapshot11 = makeSnapshot("test11");
    assertThat(metadataStore.put(ephemeralNode, serDe.toJsonStr(snapshot11)).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.get("enode").get()).isEqualTo(snapshot11));
    assertThat(cache.getInstances()).containsOnly(snapshot11, snapshot2);

    SnapshotMetadata snapshot21 = makeSnapshot("test21");
    assertThat(metadataStore.put(persistentNode, serDe.toJsonStr(snapshot21)).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.get("node").get()).isEqualTo(snapshot21));
    assertThat(cache.getInstances()).containsOnly(snapshot11, snapshot21);

    final String ephemeralNode2 = "/root/enode2";
    SnapshotMetadata snapshot12 = makeSnapshot("test12");
    assertThat(metadataStore.createEphemeralNode(ephemeralNode2, serDe.toJsonStr(snapshot12)).get())
        .isNull();
    await().untilAsserted(() -> assertThat(cache.get("enode2").get()).isEqualTo(snapshot12));
    assertThat(cache.getInstances()).containsOnly(snapshot11, snapshot21, snapshot12);

    final String persistentNode2 = "/root/node2";
    SnapshotMetadata snapshot22 = makeSnapshot("test22");
    assertThat(metadataStore.create(persistentNode2, serDe.toJsonStr(snapshot22), true).get())
        .isNull();
    await().untilAsserted(() -> assertThat(cache.get("node2").get()).isEqualTo(snapshot22));
    assertThat(cache.getInstances()).containsOnly(snapshot11, snapshot21, snapshot12, snapshot22);

    assertThat(metadataStore.delete(ephemeralNode2).get()).isNull();
    await().untilAsserted(() -> assertThat(cache.getInstances().size()).isEqualTo(3));
    assertThat(cache.getInstances()).containsOnly(snapshot11, snapshot21, snapshot22);

    // Closing the curator connection expires the ephemeral node and cache is left with
    // persistent node.
    metadataStore.close();
    await().untilAsserted(() -> assertThat(cache.getInstances().size()).isEqualTo(2));
    assertThat(cache.getInstances()).containsOnly(snapshot21, snapshot22);

    cache.close();
  }
}
