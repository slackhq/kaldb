package com.slack.kaldb.metadata.search;

import com.slack.kaldb.metadata.core.EphemeralMutableMetadataStore;
import com.slack.kaldb.metadata.core.MetadataSerializer;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SearchMetadataStore extends EphemeralMutableMetadataStore<SearchMetadata> {

  private static final Logger LOG = LoggerFactory.getLogger(SearchMetadataStore.class);

  public SearchMetadataStore(
      boolean shouldCache,
      String storeFolder,
      MetadataStore metadataStore,
      MetadataSerializer<SearchMetadata> metadataSerializer)
      throws Exception {
    super(shouldCache, false, storeFolder, metadataStore, metadataSerializer, LOG);
  }
}
