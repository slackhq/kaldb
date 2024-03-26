package com.slack.kaldb.metadata.cache;

import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.slack.kaldb.metadata.core.KaldbPartitioningMetadataStore;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class CacheSlotMetadataStore extends KaldbPartitioningMetadataStore<CacheSlotMetadata> {
  public static final String CACHE_SLOT_ZK_PATH = "/partitioned_cacheSlot";

  /**
   * Initializes a cache slot metadata store at the CACHE_SLOT_ZK_PATH. This should be used to
   * create/update the cache slots, and for listening to all cache slot events.
   */
  public CacheSlotMetadataStore(AsyncCuratorFramework curatorFramework, MeterRegistry meterRegistry)
      throws Exception {
    super(
        curatorFramework,
        CreateMode.EPHEMERAL,
        new CacheSlotMetadataSerializer().toModelSerializer(),
        CACHE_SLOT_ZK_PATH,
        meterRegistry);
  }

  /** Update the cache slot state, if the slot is not FREE. */
  public ListenableFuture<?> updateNonFreeCacheSlotState(
      final CacheSlotMetadata cacheSlotMetadata,
      final Metadata.CacheSlotMetadata.CacheSlotState slotState) {

    if (cacheSlotMetadata.cacheSlotState.equals(Metadata.CacheSlotMetadata.CacheSlotState.FREE)) {
      throw new IllegalArgumentException(
          "Current state of slot can't be free: " + cacheSlotMetadata.name);
    }
    String replicaId =
        slotState.equals(Metadata.CacheSlotMetadata.CacheSlotState.FREE)
            ? ""
            : cacheSlotMetadata.replicaId;
    return updateCacheSlotStateStateWithReplicaId(cacheSlotMetadata, slotState, replicaId);
  }

  /** Update CacheSlotState and replicaId fields while keeping the remaining fields the same. */
  public ListenableFuture<?> updateCacheSlotStateStateWithReplicaId(
      final CacheSlotMetadata cacheSlotMetadata,
      final Metadata.CacheSlotMetadata.CacheSlotState newState,
      final String replicaId) {
    CacheSlotMetadata updatedChunkMetadata =
        new CacheSlotMetadata(
            cacheSlotMetadata.name,
            newState,
            replicaId,
            Instant.now().toEpochMilli(),
            cacheSlotMetadata.supportedIndexTypes,
            cacheSlotMetadata.hostname,
            cacheSlotMetadata.replicaSet);
    // todo - consider refactoring this to return a completable future instead
    return JdkFutureAdapters.listenInPoolThread(
        updateAsync(updatedChunkMetadata).toCompletableFuture());
  }
}
