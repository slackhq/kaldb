package com.slack.kaldb.recovery;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import brave.Tracing;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.server.MetadataStoreService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RecoveryServiceTest {

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  @Before
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();
  }

  @After
  public void shutdown() throws IOException {
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldRegisterAvailableRecoveryNode() throws Exception {
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("shouldRegisterAvailableRecoveryNode")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    KaldbConfigs.ServerConfig serverConfig =
        KaldbConfigs.ServerConfig.newBuilder()
            .setServerPort(1234)
            .setServerAddress("localhost")
            .build();

    KaldbConfigs.RecoveryConfig recoveryConfig =
        KaldbConfigs.RecoveryConfig.newBuilder().setServerConfig(serverConfig).build();

    SearchContext searchContext = SearchContext.fromConfig(serverConfig);

    MetadataStoreService metadataStoreService = new MetadataStoreService(meterRegistry, zkConfig);
    metadataStoreService.startAsync();
    metadataStoreService.awaitRunning(DEFAULT_START_STOP_DURATION);

    RecoveryService recoveryService =
        new RecoveryService(recoveryConfig, metadataStoreService, meterRegistry);
    recoveryService.startAsync();
    recoveryService.awaitRunning(DEFAULT_START_STOP_DURATION);

    RecoveryNodeMetadataStore recoveryNodeMetadataStore =
        new RecoveryNodeMetadataStore(metadataStoreService.getMetadataStore(), false);

    RecoveryNodeMetadata recoveryNodeMetadata =
        recoveryNodeMetadataStore.getNodeSync(searchContext.hostname);
    assertThat(recoveryNodeMetadata.recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);

    metadataStoreService.stopAsync();
    metadataStoreService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }
}
