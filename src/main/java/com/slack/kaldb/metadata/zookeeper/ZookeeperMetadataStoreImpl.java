package com.slack.kaldb.metadata.zookeeper;

import static com.slack.kaldb.util.ArgValidationUtils.ensureNonEmptyString;
import static com.slack.kaldb.util.ArgValidationUtils.ensureTrue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.kaldb.metadata.core.KaldbMetadata;
import com.slack.kaldb.metadata.core.MetadataSerializer;
import com.slack.kaldb.util.FatalErrorHandler;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZookeeperMetadataStore implements a store in zookeeper. This class interacts with ZK using the
 * apache curator library. It also abstracts it's users from the details of ZK and provides a
 * simpler higher level API.
 *
 * <p>All the public calls in this class are executed in it's own thread pool which makes the API
 * non-blocking while allowing for parallelism. The actual methods are implemented as private
 * methods ending in Impl. A corresponding public method is exposed that executes these calls in a
 * thread pool.
 *
 * <p>NOTE: We will use the async as a suffix for methods which run in the background in curator.
 * So, we don't use it for calls that run in a future pool even though they are technically async.
 */
public class ZookeeperMetadataStoreImpl implements MetadataStore {
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperMetadataStoreImpl.class);

  public static final String METADATA_FAILED_COUNTER = "metadata.failed";
  public static final String ZK_FAILED_COUNTER = "metadata.failed.zk";
  public static final String METADATA_WRITE_COUNTER = "metadata.write";
  public static final String METADATA_READ_COUNTER = "metadata.read";

  private final CuratorFramework curator;
  private final Counter failureCounter;
  private final Counter zkFailureCounter;
  private final Counter metadataWriteCounter;
  private final Counter metadataReadCounter;

  // A thread pool to run all the metadata store operations in.
  private final ListeningExecutorService metadataExecutorService;

  @SuppressWarnings("UnstableApiUsage")
  public ZookeeperMetadataStoreImpl(
      String zkHostPath,
      String zkPathPrefix,
      int sessionTimeoutMs,
      int connectionTimeoutMs,
      RetryPolicy retryPolicy,
      FatalErrorHandler fatalErrorHandler,
      MeterRegistry meterRegistry) {
    ensureNonEmptyString(zkHostPath, "zkHostPath can't be null or empty");
    ensureNonEmptyString(zkPathPrefix, "zkPathPrefix can't be null or empty");
    ensureTrue(sessionTimeoutMs > 0, "sessionTimeoutMs should be a positive number");
    ensureTrue(connectionTimeoutMs > 0, "connectionTimeoutMs should be a positive number");

    this.failureCounter = meterRegistry.counter(METADATA_FAILED_COUNTER);
    this.zkFailureCounter = meterRegistry.counter(ZK_FAILED_COUNTER);
    this.metadataWriteCounter = meterRegistry.counter(METADATA_WRITE_COUNTER);
    this.metadataReadCounter = meterRegistry.counter(METADATA_READ_COUNTER);

    // TODO: Pass the thread pool in a constructor so we can use direct executor in tests if needed.
    // Create an executor service that runs all the ZK operations
    ThreadFactory metadataStoreThreadFactory =
        new ThreadFactoryBuilder().setNameFormat("kaldb-metadata-store-pool").build();
    ThreadPoolExecutor executor =
        (ThreadPoolExecutor) Executors.newCachedThreadPool(metadataStoreThreadFactory);
    this.metadataExecutorService =
        MoreExecutors.listeningDecorator(
            MoreExecutors.getExitingExecutorService(executor, 1, TimeUnit.SECONDS));

    // TODO: In future add ZK auth credentials can be passed in here.
    this.curator =
        CuratorFrameworkFactory.builder()
            .connectString(zkHostPath)
            .namespace(zkPathPrefix)
            .connectionTimeoutMs(connectionTimeoutMs)
            .sessionTimeoutMs(sessionTimeoutMs)
            .retryPolicy(retryPolicy)
            .build();

    // A catch-all handler for any errors we may have missed.
    curator
        .getUnhandledErrorListenable()
        .addListener(
            (message, exception) -> {
              LOG.error("Unhandled error {} could be a possible bug", message, exception);
              failureCounter.increment();
              throw new RuntimeException(exception);
            });

    // Log all connection state changes to help debugging.
    curator
        .getConnectionStateListenable()
        .addListener(
            (curator, connectionState) ->
                LOG.info("Curator connection state changed to {}", connectionState));

    /*
     * If a ZK session expires, we need to create all the watches and ephemeral nodes again.
     * In such a case, any ephemeral nodes and watches would expire and need to be re-created.
     * To keep it simple for now, we terminate the process and let the process initialization
     * register those nodes.
     */
    curator
        .getCuratorListenable()
        .addListener(
            (curator, curatorEvent) -> {
              if (curatorEvent.getType() == CuratorEventType.WATCHED
                  && curatorEvent.getWatchedEvent().getState()
                      == Watcher.Event.KeeperState.Expired) {
                LOG.warn("The ZK session has expired {}.", curatorEvent);
                fatalErrorHandler.handleFatal(new Throwable("ZK session expired."));
              }
            });

    curator.start();
    LOG.info(
        "Started curator server with the following config zkhost: {}, path prefix: {}, "
            + "connection timeout ms: {}, session timeout ms {} and retry policy {}",
        zkHostPath,
        zkPathPrefix,
        connectionTimeoutMs,
        sessionTimeoutMs,
        retryPolicy);
  }

  public void close() {
    LOG.info("Closing curator connection.");
    curator.close();
    LOG.info("Closed curator connection successfully.");
  }

  private void createEphemeralNodeImpl(String path, String data) {
    metadataWriteCounter.increment();
    LOG.info("Creating ephemeral node at {} with data {}", path, data);
    try {
      curator.create().withMode(CreateMode.EPHEMERAL).forPath(path, data.getBytes());
    } catch (KeeperException.NodeExistsException e) {
      throw new NodeExistsException(path);
    } catch (KeeperException e) {
      zkFailureCounter.increment();
      LOG.warn("Failed with ZK exception when writing at path {}", path, e);
      throw new InternalMetadataStoreException("Creating an ephemeral node at " + path);
    } catch (Exception e) {
      failureCounter.increment();
      LOG.error("Failed with an unknown error {}", e.getMessage(), e);
      throw new InternalMetadataStoreException("Creating node at path " + path);
    }
  }

  /** Create an ephemeral node at path */
  @Override
  public ListenableFuture<?> createEphemeralNode(String path, String data) {
    return metadataExecutorService.submit(() -> createEphemeralNodeImpl(path, data));
  }

  private void createImpl(String path, String data, boolean createMissingParents) {
    try {
      LOG.info("Creating a node at {} with data {}.", path, data);
      metadataWriteCounter.increment();
      if (createMissingParents) {
        curator
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(path, data.getBytes());
      } else {
        curator.create().withMode(CreateMode.PERSISTENT).forPath(path, data.getBytes());
      }
    } catch (KeeperException.NodeExistsException e) {
      throw new NodeExistsException(path);
    } catch (KeeperException e) {
      zkFailureCounter.increment();
      LOG.warn("Failed with a ZK exception when writing to path {}", path, e);
      throw new InternalMetadataStoreException("Creating a node at path " + path);
    } catch (Exception e) {
      failureCounter.increment();
      LOG.error("Failed with unknown error {}", e.getMessage(), e);
      throw new InternalMetadataStoreException("Creating a node at path " + path);
    }
  }

  /**
   * Create a persistent node at path. If createMissingParents is true, create the missing parent
   * folders also to simplify node creation. This is also efficient since we can create a node with
   * one call to ZK instead of several calls.
   */
  @Override
  public ListenableFuture<?> create(String path, String data, boolean createMissingParents) {
    return metadataExecutorService.submit(() -> createImpl(path, data, createMissingParents));
  }

  private Boolean existsImpl(String path) {
    Stat result;
    try {
      metadataReadCounter.increment();
      LOG.debug("Checking existence of a node at path {}", path);
      result = curator.checkExists().forPath(path);
    } catch (KeeperException e) {
      zkFailureCounter.increment();
      LOG.warn("Failed with ZK exception when reading from path {}", path, e);
      throw new InternalMetadataStoreException("Checking exists for path " + path);
    } catch (Exception e) {
      failureCounter.increment();
      LOG.error("Failed with unknown error {}", e.getMessage(), e);
      throw new InternalMetadataStoreException("Checking exists for path " + path);
    }
    return result != null;
  }

  /** Check if a path exists in store. */
  @Override
  public ListenableFuture<Boolean> exists(String path) {
    return metadataExecutorService.submit(() -> existsImpl(path));
  }

  /** Store data in path. Throw exception if node doesn't exist. */
  private void putImpl(String path, String data) {
    try {
      metadataWriteCounter.increment();
      LOG.info("Setting data for node at {} to {}", path, data);
      curator.setData().forPath(path, data.getBytes());
    } catch (KeeperException.NoNodeException e) {
      throw new NoNodeException(path);
    } catch (KeeperException e) {
      zkFailureCounter.increment();
      LOG.warn("Failed with an error when updating node at path {}", path, e);
      throw new InternalMetadataStoreException("Updating node at path " + path);
    } catch (Exception e) {
      failureCounter.increment();
      LOG.error("Failed with an unknown error {}", e.getMessage(), e);
      throw new InternalMetadataStoreException("Updating node at path " + path);
    }
  }

  @Override
  public ListenableFuture<?> put(String path, String data) {
    return metadataExecutorService.submit(() -> putImpl(path, data));
  }

  // TODO: Consider fetching the data in background if it results in better perf due to batching.
  private String getImpl(String path) {
    String result;
    try {
      metadataReadCounter.increment();
      LOG.debug("Fetching data for node at {}", path);
      byte[] data = curator.getData().forPath(path);
      if (data != null) {
        result = new String(data);
      } else {
        throw new InternalMetadataStoreException("Get returned no data");
      }
    } catch (KeeperException.NoNodeException e) {
      throw new NoNodeException(path);
    } catch (KeeperException e) {
      zkFailureCounter.increment();
      LOG.warn("Failed with a ZK exception when getting path {}", path, e);
      throw new InternalMetadataStoreException("Get a node at path " + path);
    } catch (Exception e) {
      failureCounter.increment();
      LOG.error("Failed with an unknown error {}", e.getMessage(), e);
      throw new InternalMetadataStoreException("Fetching node at " + path);
    }
    return result;
  }

  @Override
  public ListenableFuture<String> get(String path) {
    return metadataExecutorService.submit(() -> getImpl(path));
  }

  private void deleteImpl(String path) {
    try {
      metadataWriteCounter.increment();
      LOG.info("Deleting node at {}", path);
      curator.delete().forPath(path);
    } catch (KeeperException.NoNodeException e) {
      throw new NoNodeException(path);
    } catch (KeeperException e) {
      zkFailureCounter.increment();
      LOG.warn("Failed with a ZK exception when deleting node at {}", path, e);
      throw new InternalMetadataStoreException("Deleting node at " + path);
    } catch (Exception e) {
      failureCounter.increment();
      LOG.error("Failed with an unknown error {}", e.getMessage(), e);
      throw new InternalMetadataStoreException("Deleting node at " + path);
    }
  }

  @Override
  public ListenableFuture<?> delete(String path) {
    return metadataExecutorService.submit(() -> deleteImpl(path));
  }

  private List<String> getChildrenImpl(String path) {
    List<String> result;
    try {
      metadataReadCounter.increment();
      result = new ArrayList<>(curator.getChildren().forPath(path));
    } catch (KeeperException.NoNodeException e) {
      throw new NoNodeException(path);
    } catch (KeeperException e) {
      zkFailureCounter.increment();
      LOG.warn("Failed with a ZK exception when getting children at {}", path, e);
      throw new InternalMetadataStoreException("Getting children for " + path);
    } catch (Exception e) {
      failureCounter.increment();
      LOG.error("Failed with an unknown error {}", e.getMessage(), e);
      throw new InternalMetadataStoreException("Fetching children at " + path);
    }
    return result;
  }

  @Override
  public ListenableFuture<List<String>> getChildren(String path) {
    return metadataExecutorService.submit(() -> getChildrenImpl(path));
  }

  /**
   * This implementation uses a CachedMetadataStore, a wrapper on curator cache, to cache all the
   * nodes under a given path.
   */
  @Override
  public <T extends KaldbMetadata> CachedMetadataStore<T> cacheNodeAndChildren(
      String path, MetadataSerializer<T> metadataSerializer) {
    if (!existsImpl(path)) {
      throw new NoNodeException("Node doesn't exist at path: " + path);
    }
    return new CachedMetadataStoreImpl<>(
        path, metadataSerializer, curator, metadataExecutorService);
  }

  @VisibleForTesting
  public CuratorFramework getCurator() {
    return curator;
  }

  @VisibleForTesting
  public ListeningExecutorService getMetadataExecutorService() {
    return metadataExecutorService;
  }
}
