package com.slack.kaldb.metadata.search;

import com.slack.kaldb.metadata.core.KaldbMetadataStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

public class SearchMetadataStore extends KaldbMetadataStore<SearchMetadata> {
  public static final String SEARCH_METADATA_STORE_ZK_PATH = "/search";

  public SearchMetadataStore(
      AsyncCuratorFramework curatorFramework, boolean shouldCache, MeterRegistry meterRegistry)
      throws Exception {
    super(
        curatorFramework,
        CreateMode.EPHEMERAL,
        shouldCache,
        new SearchMetadataSerializer().toModelSerializer(),
        SEARCH_METADATA_STORE_ZK_PATH,
        meterRegistry);
  }

  @Override
  public AsyncStage<Stat> updateAsync(SearchMetadata metadataNode) {
    throw new UnsupportedOperationException("Updates are not permitted for search metadata");
  }
}
