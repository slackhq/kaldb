package com.slack.kaldb.metadata.core;

import com.slack.kaldb.metadata.zookeeper.CachedMetadataStore;
import com.slack.kaldb.metadata.zookeeper.CachedMetadataStoreListener;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;

/**
 * This store adds a cache on top of the KaldbMetadataStore. Optionally, this class can disable
 * caching by passing in a feature flag.
 *
 * <p>Instantiating an instance of this class creates the cache, adds a listener and starts the
 * cache. The listener doesn't notify the watchers on cache initialization but fires on subsequent
 * cache events. To close the cache, call the close method.
 */
public abstract class CacheableMetadataStore<T extends KaldbMetadata>
    extends KaldbMetadataStore<T> {

  private final Optional<CachedMetadataStore<T>> cache;

  private final List<KaldbMetadataStoreChangeListener> watchers;

  public CacheableMetadataStore(
      boolean shouldCache,
      String snapshotStoreFolder,
      MetadataStore metadataStore,
      MetadataSerializer<T> metadataSerializer,
      Logger logger)
      throws Exception {

    super(metadataStore, snapshotStoreFolder, metadataSerializer, logger);
    watchers = new ArrayList<>();
    if (shouldCache) {
      CachedMetadataStore<T> localCache =
          metadataStore.cacheNodeAndChildren(snapshotStoreFolder, metadataSerializer);

      // Notify listeners on cache change.
      localCache.addListener(
          new CachedMetadataStoreListener() {
            @Override
            public void cacheChanged() {
              notifyListeners();
            }

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
              notifyListeners();
            }

            private void notifyListeners() {
              for (KaldbMetadataStoreChangeListener watcher : watchers)
                try {
                  watcher.onMetadataStoreChanged();
                } catch (Exception e) {
                  logger.error("Encountered exception when invoking a watcher:", e);
                }
            }
          });
      localCache.start();
      cache = Optional.of(localCache);
      logger.info("Caching nodes for path {}", snapshotStoreFolder);
    } else {
      cache = Optional.empty();
      logger.info("Disabled caching nodes under {}", snapshotStoreFolder);
    }
  }

  public void start() throws Exception {
    if (cache.isEmpty()) throw new UnsupportedOperationException("Caching is disabled");
    else cache.get().start();
  }

  public void close() {
    if (cache.isEmpty()) {
      throw new UnsupportedOperationException();
    } else {
      cache.get().close();
      watchers.clear();
    }
  }

  public List<T> getCached() {
    if (cache.isEmpty()) throw new UnsupportedOperationException("Caching is disabled");
    else return cache.get().getInstances();
  }

  public void addListener(KaldbMetadataStoreChangeListener watcher) {
    if (cache.isEmpty()) throw new UnsupportedOperationException("Caching is disabled");
    else watchers.add(watcher);
  }

  public void removeListener(KaldbMetadataStoreChangeListener watcher) {
    if (cache.isEmpty()) throw new UnsupportedOperationException("Caching is disabled.");
    else watchers.remove(watcher);
  }
}
