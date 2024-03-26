package com.slack.kaldb.metadata.dataset;

import com.slack.kaldb.metadata.core.KaldbMetadataStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class DatasetMetadataStore extends KaldbMetadataStore<DatasetMetadata> {
  // TODO: The path should be dataset, but leaving it as /service for backwards compatibility.
  public static final String DATASET_METADATA_STORE_ZK_PATH = "/service";

  public DatasetMetadataStore(
      AsyncCuratorFramework curator, boolean shouldCache, MeterRegistry meterRegistry)
      throws Exception {
    super(
        curator,
        CreateMode.PERSISTENT,
        shouldCache,
        new DatasetMetadataSerializer().toModelSerializer(),
        DATASET_METADATA_STORE_ZK_PATH,
        meterRegistry);
  }
}
