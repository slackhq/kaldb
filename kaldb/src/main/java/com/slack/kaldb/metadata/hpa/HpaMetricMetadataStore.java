package com.slack.kaldb.metadata.hpa;

import com.slack.kaldb.metadata.core.KaldbMetadataStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class HpaMetricMetadataStore extends KaldbMetadataStore<HpaMetricMetadata> {
  public static final String AUTOSCALER_METADATA_STORE_ZK_PATH = "/hpa_metrics";

  public HpaMetricMetadataStore(
      AsyncCuratorFramework curator, boolean shouldCache, MeterRegistry meterRegistry) {
    super(
        curator,
        CreateMode.EPHEMERAL,
        shouldCache,
        new HpaMetricMetadataSerializer().toModelSerializer(),
        AUTOSCALER_METADATA_STORE_ZK_PATH,
        meterRegistry);
  }
}
