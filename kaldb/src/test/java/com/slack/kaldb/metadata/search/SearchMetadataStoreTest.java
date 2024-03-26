package com.slack.kaldb.metadata.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SearchMetadataStoreTest {
  private SimpleMeterRegistry meterRegistry;
  private TestingServer testingServer;
  private AsyncCuratorFramework curatorFramework;
  private SearchMetadataStore store;

  @BeforeEach
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
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
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (store != null) store.close();
    curatorFramework.unwrap().close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void testSearchMetadataStoreIsNotUpdatable() throws Exception {
    store = new SearchMetadataStore(curatorFramework, true, meterRegistry);
    SearchMetadata searchMetadata = new SearchMetadata("test", "snapshot", "http");
    Throwable exAsync = catchThrowable(() -> store.updateAsync(searchMetadata));
    assertThat(exAsync).isInstanceOf(UnsupportedOperationException.class);

    Throwable exSync = catchThrowable(() -> store.updateSync(searchMetadata));
    assertThat(exSync).isInstanceOf(UnsupportedOperationException.class);
  }
}
