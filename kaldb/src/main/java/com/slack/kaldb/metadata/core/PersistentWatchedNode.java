package com.slack.kaldb.metadata.core;

import com.slack.kaldb.util.RuntimeHalterImpl;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.recipes.watch.PersistentWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistentWatchedNode implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(PersistentWatchedNode.class);
  private final PersistentNode node;
  private PersistentWatcher watcher;

  public static final String PERSISTENT_NODE_RECREATED_COUNTER =
      "metadata_persistent_node_recreated";
  private final Counter persistentNodeRecreatedCounter;

  private final CuratorFramework givenClient;

  public PersistentWatchedNode(
      CuratorFramework givenClient,
      final CreateMode mode,
      boolean useProtection,
      final String basePath,
      byte[] initData,
      MeterRegistry meterRegistry) {
    this.givenClient = givenClient;
    persistentNodeRecreatedCounter = meterRegistry.counter(PERSISTENT_NODE_RECREATED_COUNTER);
    node = new PersistentNode(givenClient, mode, useProtection, basePath, initData);
    node.getListenable().addListener(_ -> persistentNodeRecreatedCounter.increment());
  }

  private Watcher nodeWatcher() {
    return event -> {
      try {
        if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
          // get data
          givenClient
              .getData()
              .inBackground(
                  (_, event1) -> {
                    if (node.getActualPath() != null) {
                      byte[] updatedBytes = event1.getData();
                      if (!Arrays.equals(node.getData(), updatedBytes)) {
                        // only trigger a setData if something actually
                        // changed, otherwise we end up in a deathloop
                        node.setData(updatedBytes);
                      }
                    }
                  })
              .forPath(event.getPath());
        }
      } catch (Exception e) {
        LOG.info("Error", e);
        new RuntimeHalterImpl().handleFatal(e);
      }
    };
  }

  public void start() {
    node.start();
    try {
      node.waitForInitialCreate(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    watcher = new PersistentWatcher(givenClient, node.getActualPath(), false);
    watcher.start();
    watcher.getListenable().addListener(nodeWatcher());
  }

  public void setData(byte[] data) throws Exception {
    node.setData(data);
  }

  public byte[] getData() {
    return node.getData();
  }

  @Override
  public void close() throws IOException {
    if (watcher != null) {
      watcher.close();
    }
    node.close();
  }
}
