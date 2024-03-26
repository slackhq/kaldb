package com.slack.kaldb.server;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.hpa.HpaMetricMetadata;
import com.slack.kaldb.metadata.hpa.HpaMetricMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.List;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HpaMetricPublisherServiceTest {

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;
  private AsyncCuratorFramework curatorFramework;
  private HpaMetricMetadataStore hpaMetricMetadataStore;

  @BeforeEach
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("HpaMetricPublisherServiceTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    curatorFramework = CuratorBuilder.build(new SimpleMeterRegistry(), zkConfig);
    hpaMetricMetadataStore = spy(new HpaMetricMetadataStore(curatorFramework, true, meterRegistry));
  }

  @AfterEach
  public void shutdown() throws IOException {
    hpaMetricMetadataStore.close();
    curatorFramework.unwrap().close();

    testingServer.close();
    meterRegistry.close();
  }

  @Test
  void shouldRegisterMetersAsAdded() throws Exception {
    HpaMetricPublisherService hpaMetricPublisherService =
        new HpaMetricPublisherService(
            hpaMetricMetadataStore, meterRegistry, Metadata.HpaMetricMetadata.NodeRole.CACHE);
    hpaMetricPublisherService.startUp();

    List<Meter> initialMeters = meterRegistry.getMeters();

    hpaMetricMetadataStore.createSync(
        new HpaMetricMetadata("foo", Metadata.HpaMetricMetadata.NodeRole.CACHE, 1.0));

    await().until(() -> hpaMetricMetadataStore.listSync().size() == 1);
    await().until(() -> meterRegistry.getMeters().size() == 1 + initialMeters.size());

    hpaMetricMetadataStore.createSync(
        new HpaMetricMetadata("bar", Metadata.HpaMetricMetadata.NodeRole.INDEX, 1.0));

    await().until(() -> hpaMetricMetadataStore.listSync().size() == 2);
    await().until(() -> meterRegistry.getMeters().size() == 1 + initialMeters.size());

    hpaMetricMetadataStore.createSync(
        new HpaMetricMetadata("baz", Metadata.HpaMetricMetadata.NodeRole.CACHE, 0.0));

    await().until(() -> hpaMetricMetadataStore.listSync().size() == 3);
    await().until(() -> meterRegistry.getMeters().size() == 2 + initialMeters.size());
  }
}
