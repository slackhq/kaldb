package com.slack.kaldb.metadata.snapshot;

import com.slack.kaldb.metadata.core.KaldbPartitioningMetadataStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class SnapshotMetadataStore extends KaldbPartitioningMetadataStore<SnapshotMetadata> {
  public static final String SNAPSHOT_METADATA_STORE_ZK_PATH = "/partitioned_snapshot";

  // TODO: Consider restricting the update methods to only update live nodes only?

  public SnapshotMetadataStore(AsyncCuratorFramework curatorFramework, MeterRegistry meterRegistry)
      throws Exception {
    super(
        curatorFramework,
        CreateMode.PERSISTENT,
        new SnapshotMetadataSerializer().toModelSerializer(),
        SNAPSHOT_METADATA_STORE_ZK_PATH,
        meterRegistry);
  }
}
