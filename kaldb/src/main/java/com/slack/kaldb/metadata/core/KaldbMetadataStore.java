package com.slack.kaldb.metadata.core;

import static com.slack.kaldb.server.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;

import com.slack.kaldb.util.RuntimeHalterImpl;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.recipes.watch.PersistentWatcher;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.curator.x.async.modeled.cached.ModeledCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KaldbMetadataStore is a class which provides consistent ZK apis for all the metadata store class.
 *
 * <p>Every method provides an async and a sync API. In general, use the async API you are
 * performing batch operations and a sync if you are performing a synchronous operation on a node.
 *
 * <p><a href="https://curator.apache.org/docs/recipes-persistent-node">Persistent node recipie</a>
 */
public class KaldbMetadataStore<T extends KaldbMetadata> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbMetadataStore.class);

  public static String PERSISTENT_EPHEMERAL_PROPERTY = "kaldb.metadata.persistentEphemeral";
  protected final String storeFolder;

  private final ZPath zPath;

  private final CreateMode createMode;

  private final AsyncCuratorFramework curator;

  private final ModelSpec<T> modelSpec;

  private final CountDownLatch cacheInitialized = new CountDownLatch(1);

  protected final ModeledFramework<T> modeledClient;

  private final CachedModeledFramework<T> cachedModeledFramework;

  private final Map<KaldbMetadataStoreChangeListener<T>, ModeledCacheListener<T>> listenerMap =
      new ConcurrentHashMap<>();

  private final Map<String, PersistentNode> persistentNodeMap = new ConcurrentHashMap<>();
  private final Map<String, PersistentWatcher> persistentWatcherMap = new ConcurrentHashMap<>();

  public static final String PERSISTENT_NODE_RECREATED_COUNTER =
      "metadata_persistent_node_recreated";
  private final Counter persistentNodeRecreatedCounter;

  public KaldbMetadataStore(
      AsyncCuratorFramework curator,
      CreateMode createMode,
      boolean shouldCache,
      ModelSerializer<T> modelSerializer,
      String storeFolder,
      MeterRegistry meterRegistry) {

    this.createMode = createMode;
    this.curator = curator;
    this.storeFolder = storeFolder;
    this.zPath = ZPath.parseWithIds(String.format("%s/{name}", storeFolder));

    this.modelSpec =
        ModelSpec.builder(modelSerializer)
            .withPath(zPath)
            .withCreateOptions(
                Set.of(CreateOption.createParentsIfNeeded, CreateOption.createParentsAsContainers))
            .withCreateMode(createMode)
            .build();
    modeledClient = ModeledFramework.wrap(curator, modelSpec);

    if (shouldCache) {
      cachedModeledFramework = modeledClient.cached();
      cachedModeledFramework.listenable().addListener(getCacheInitializedListener());
      cachedModeledFramework.start();
    } else {
      cachedModeledFramework = null;
    }

    persistentNodeRecreatedCounter = meterRegistry.counter(PERSISTENT_NODE_RECREATED_COUNTER);
    LOG.info(
        "Persistent ephemeral mode '{}' enabled - {}",
        PERSISTENT_EPHEMERAL_PROPERTY,
        persistentEphemeralModeEnabled());
  }

  public static boolean persistentEphemeralModeEnabled() {
    return Boolean.parseBoolean(System.getProperty(PERSISTENT_EPHEMERAL_PROPERTY, "true"));
  }

  public CompletionStage<String> createAsync(T metadataNode) {
    if (createMode == CreateMode.EPHEMERAL && persistentEphemeralModeEnabled()) {
      String nodePath = resolvePath(metadataNode);
      return hasAsync(metadataNode.name)
          .thenApplyAsync(
              (stat) -> {
                // it is possible that we have a node that hasn't been yet async persisted to ZK
                if (stat != null || persistentNodeMap.containsKey(nodePath)) {
                  throw new CompletionException(
                      new IllegalArgumentException(
                          String.format("Node already exists at '%s'", nodePath)));
                }

                PersistentNode node =
                    new PersistentNode(
                        curator.unwrap(),
                        createMode,
                        false,
                        nodePath,
                        modelSpec.serializer().serialize(metadataNode));
                persistentNodeMap.put(nodePath, node);
                node.start();

                try {
                  node.waitForInitialCreate(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
                  node.getListenable().addListener(_ -> persistentNodeRecreatedCounter.increment());

                  // add a persistent watcher for node data changes on this persistent ephemeral
                  // node this is so when someone else updates a field on the ephemeral node, the
                  // owner also updates their local copy
                  PersistentWatcher persistentWatcher =
                      new PersistentWatcher(curator.unwrap(), node.getActualPath(), false);
                  persistentWatcher
                      .getListenable()
                      .addListener(
                          event -> {
                            try {
                              if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                                node.waitForInitialCreate(
                                    DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
                                modeledClient
                                    .withPath(ZPath.parse(event.getPath()))
                                    .read()
                                    .thenAcceptAsync(
                                        (updated) -> {
                                          try {
                                            if (node.getActualPath() != null) {
                                              byte[] updatedBytes =
                                                  modelSpec.serializer().serialize(updated);
                                              if (!Arrays.equals(node.getData(), updatedBytes)) {
                                                // only trigger a setData if something actually
                                                // changed, otherwise
                                                // we end up in a deathloop
                                                node.setData(
                                                    modelSpec.serializer().serialize(updated));
                                              }
                                            }
                                          } catch (Exception e) {
                                            LOG.error(
                                                "Error attempting to set local node data - fatal ZK error",
                                                e);
                                            new RuntimeHalterImpl().handleFatal(e);
                                          }
                                        });
                              }
                            } catch (Exception e) {
                              LOG.error(
                                  "Error attempting to watch NodeDataChanged - fatal ZK error", e);
                              new RuntimeHalterImpl().handleFatal(e);
                            }
                          });
                  persistentWatcherMap.put(nodePath, persistentWatcher);
                  persistentWatcher.start();
                  return nodePath;
                } catch (Exception e) {
                  throw new CompletionException(e);
                }
              });
    } else {
      // by passing the version 0, this will throw if we attempt to create and it already exists
      return modeledClient.set(metadataNode, 0);
    }
  }

  /**
   * Based off of the private ModelFrameWorkImp resolveForSet
   *
   * @see org.apache.curator.x.async.modeled.details.ModeledFrameworkImpl.resolveForSet
   */
  private String resolvePath(T model) {
    if (modelSpec.path().isResolved()) {
      return modelSpec.path().fullPath();
    }
    return modelSpec.path().resolved(model).fullPath();
  }

  public void createSync(T metadataNode) {
    try {
      createAsync(metadataNode)
          .toCompletableFuture()
          .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error creating node " + metadataNode, e);
    }
  }

  public CompletionStage<T> getAsync(String path) {
    PersistentNode node = getPersistentNodeIfExists(path);
    if (node != null) {
      return CompletableFuture.supplyAsync(
          () -> modelSpec.serializer().deserialize(node.getData()));
    }
    if (cachedModeledFramework != null) {
      return cachedModeledFramework.withPath(zPath.resolved(path)).readThrough();
    }
    return modeledClient.withPath(zPath.resolved(path)).read();
  }

  public T getSync(String path) {
    try {
      return getAsync(path).toCompletableFuture().get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error fetching node at path " + path, e);
    }
  }

  public CompletionStage<Stat> hasAsync(String path) {
    // We don't use the persist node here, as we want to get the actual stat details which isn't
    // available on the persistentnode
    if (cachedModeledFramework != null) {
      awaitCacheInitialized();
      return cachedModeledFramework.withPath(zPath.resolved(path)).checkExists();
    }
    return modeledClient.withPath(zPath.resolved(path)).checkExists();
  }

  public boolean hasSync(String path) {
    try {
      return hasAsync(path).toCompletableFuture().get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS)
          != null;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error fetching node at path " + path, e);
    }
  }

  public CompletionStage<Stat> updateAsync(T metadataNode) {
    PersistentNode node = getPersistentNodeIfExists(metadataNode);
    if (node != null) {
      try {
        node.waitForInitialCreate(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
        node.setData(modelSpec.serializer().serialize(metadataNode));
        return CompletableFuture.completedFuture(null);
      } catch (Exception e) {
        throw new CompletionException(e);
      }
    } else {
      return modeledClient.update(metadataNode);
    }
  }

  public void updateSync(T metadataNode) {
    try {
      updateAsync(metadataNode)
          .toCompletableFuture()
          .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error updating node: " + metadataNode, e);
    }
  }

  public CompletionStage<Void> deleteAsync(String path) {
    PersistentNode node = removePersistentNodeIfExists(path);
    if (node != null) {
      try {
        node.waitForInitialCreate(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
        node.close();
        return CompletableFuture.completedFuture(null);
      } catch (Exception e) {
        throw new CompletionException(e);
      }
    } else {
      return modeledClient.withPath(zPath.resolved(path)).delete();
    }
  }

  public void deleteSync(String path) {
    try {
      deleteAsync(path).toCompletableFuture().get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error deleting node under at path: " + path, e);
    }
  }

  public CompletionStage<Void> deleteAsync(T metadataNode) {
    PersistentNode node = removePersistentNodeIfExists(metadataNode);
    if (node != null) {
      try {
        node.waitForInitialCreate(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
        node.close();
        return CompletableFuture.completedFuture(null);
      } catch (Exception e) {
        throw new CompletionException(e);
      }
    } else {
      return modeledClient.withPath(zPath.resolved(metadataNode)).delete();
    }
  }

  public void deleteSync(T metadataNode) {
    try {
      deleteAsync(metadataNode)
          .toCompletableFuture()
          .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new InternalMetadataStoreException(
          "Error deleting node under at path: " + metadataNode.name, e);
    }
  }

  public CompletionStage<List<T>> listAsync() {
    if (cachedModeledFramework == null) {
      throw new UnsupportedOperationException("Caching is disabled");
    }

    awaitCacheInitialized();
    return cachedModeledFramework.list();
  }

  public List<T> listSync() {
    try {
      return listAsync().toCompletableFuture().get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error getting cached nodes", e);
    }
  }

  public void addListener(KaldbMetadataStoreChangeListener<T> watcher) {
    if (cachedModeledFramework == null) {
      throw new UnsupportedOperationException("Caching is disabled");
    }

    // this mapping exists because the remove is by reference, and the listener is a different
    // object type
    ModeledCacheListener<T> modeledCacheListener =
        (type, path, stat, model) -> {
          // We do not expect the model to ever be null for an event on a metadata node
          if (model != null) {
            watcher.onMetadataStoreChanged(model);
          }
        };
    cachedModeledFramework.listenable().addListener(modeledCacheListener);
    listenerMap.put(watcher, modeledCacheListener);
  }

  public void removeListener(KaldbMetadataStoreChangeListener<T> watcher) {
    if (cachedModeledFramework == null) {
      throw new UnsupportedOperationException("Caching is disabled");
    }
    cachedModeledFramework.listenable().removeListener(listenerMap.remove(watcher));
  }

  private void awaitCacheInitialized() {
    try {
      cacheInitialized.await();
    } catch (InterruptedException e) {
      new RuntimeHalterImpl().handleFatal(e);
    }
  }

  private PersistentNode getPersistentNodeIfExists(T metadataNode) {
    return persistentNodeMap.getOrDefault(resolvePath(metadataNode), null);
  }

  private PersistentNode getPersistentNodeIfExists(String path) {
    return persistentNodeMap.getOrDefault(zPath.resolved(path).fullPath(), null);
  }

  private PersistentNode removePersistentNodeIfExists(T metadataNode) {
    PersistentWatcher watcher = persistentWatcherMap.remove(resolvePath(metadataNode));
    if (watcher != null) {
      watcher.close();
    }
    return persistentNodeMap.remove(resolvePath(metadataNode));
  }

  private PersistentNode removePersistentNodeIfExists(String path) {
    PersistentWatcher watcher = persistentWatcherMap.remove(zPath.resolved(path).fullPath());
    if (watcher != null) {
      watcher.close();
    }
    return persistentNodeMap.remove(zPath.resolved(path).fullPath());
  }

  private ModeledCacheListener<T> getCacheInitializedListener() {
    return new ModeledCacheListener<T>() {
      @Override
      public void accept(Type type, ZPath path, Stat stat, T model) {
        // no-op
      }

      @Override
      public void initialized() {
        ModeledCacheListener.super.initialized();
        cacheInitialized.countDown();
      }
    };
  }

  @Override
  public void close() {
    persistentWatcherMap.forEach(
        (_, persistentWatcher) -> {
          try {
            persistentWatcher.close();
          } catch (Exception e) {
            LOG.error("Error removing persistent watchers", e);
          }
        });

    persistentNodeMap.forEach(
        (_, persistentNode) -> {
          try {
            persistentNode.close();
          } catch (Exception e) {
            LOG.error("Error removing persistent nodes", e);
          }
        });

    if (cachedModeledFramework != null) {
      listenerMap.forEach(
          (_, tModeledCacheListener) ->
              cachedModeledFramework.listenable().removeListener(tModeledCacheListener));
      cachedModeledFramework.close();
    }
  }
}
