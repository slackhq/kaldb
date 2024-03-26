package com.slack.kaldb.metadata.replica;

import com.slack.kaldb.metadata.core.KaldbPartitioningMetadataStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class ReplicaMetadataStore extends KaldbPartitioningMetadataStore<ReplicaMetadata> {
  public static final String REPLICA_STORE_ZK_PATH = "/partitioned_replica";

  public ReplicaMetadataStore(AsyncCuratorFramework curatorFramework, MeterRegistry meterRegistry)
      throws Exception {
    super(
        curatorFramework,
        CreateMode.PERSISTENT,
        new ReplicaMetadataSerializer().toModelSerializer(),
        REPLICA_STORE_ZK_PATH,
        meterRegistry);
  }
}
