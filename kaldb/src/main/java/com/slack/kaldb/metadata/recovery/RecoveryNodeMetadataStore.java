package com.slack.kaldb.metadata.recovery;

import com.slack.kaldb.metadata.core.KaldbMetadataStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class RecoveryNodeMetadataStore extends KaldbMetadataStore<RecoveryNodeMetadata> {
  public static final String RECOVERY_NODE_ZK_PATH = "/recoveryNode";

  /**
   * Initializes a recovery node metadata store at the RECOVERY_NODE_ZK_PATH. This should be used to
   * create/update the recovery nodes, and for listening to all recovery node events.
   */
  public RecoveryNodeMetadataStore(
      AsyncCuratorFramework curatorFramework, boolean shouldCache, MeterRegistry meterRegistry) {
    super(
        curatorFramework,
        CreateMode.EPHEMERAL,
        shouldCache,
        new RecoveryNodeMetadataSerializer().toModelSerializer(),
        RECOVERY_NODE_ZK_PATH,
        meterRegistry);
  }

  /**
   * Initializes a recovery node metadata store at RECOVERY_NODE_ZK_PATH/{recoveryNodeName}. This
   * should be used to add listeners to specific recovery nodes, and is not expected to be used for
   * mutating any nodes.
   */
  public RecoveryNodeMetadataStore(
      AsyncCuratorFramework curatorFramework,
      String recoveryNodeName,
      boolean shouldCache,
      MeterRegistry meterRegistry) {
    super(
        curatorFramework,
        CreateMode.EPHEMERAL,
        shouldCache,
        new RecoveryNodeMetadataSerializer().toModelSerializer(),
        String.format("%s/%s", RECOVERY_NODE_ZK_PATH, recoveryNodeName),
        meterRegistry);
  }
}
