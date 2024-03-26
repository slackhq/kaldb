package com.slack.kaldb.metadata.core;

import static com.slack.kaldb.util.ArgValidationUtils.ensureNonEmptyString;
import static com.slack.kaldb.util.ArgValidationUtils.ensureTrue;

import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.util.RuntimeHalterImpl;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builder class to instantiate an common curator instance for use in the metadata stores. */
public class CuratorBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(CuratorBuilder.class);

  public static final String METADATA_FAILED_COUNTER = "metadata.failed";

  public static AsyncCuratorFramework build(
      MeterRegistry meterRegistry, KaldbConfigs.ZookeeperConfig zkConfig) {
    ensureNonEmptyString(zkConfig.getZkConnectString(), "zkConnectString can't be null or empty");
    ensureNonEmptyString(zkConfig.getZkPathPrefix(), "zkPathPrefix can't be null or empty");
    ensureTrue(
        zkConfig.getZkSessionTimeoutMs() > 0, "sessionTimeoutMs should be a positive number");
    ensureTrue(
        zkConfig.getZkConnectionTimeoutMs() > 0, "connectionTimeoutMs should be a positive number");

    Counter failureCounter = meterRegistry.counter(METADATA_FAILED_COUNTER);
    // todo - consider making the retry until elapsed a separate config from the zk session timeout
    RetryPolicy retryPolicy =
        new RetryUntilElapsed(
            zkConfig.getZkSessionTimeoutMs(), zkConfig.getSleepBetweenRetriesMs());

    // TODO: In future add ZK auth credentials can be passed in here.
    CuratorFramework curator =
        CuratorFrameworkFactory.builder()
            .connectString(zkConfig.getZkConnectString())
            .namespace(zkConfig.getZkPathPrefix())
            .connectionTimeoutMs(zkConfig.getZkConnectionTimeoutMs())
            .sessionTimeoutMs(zkConfig.getZkSessionTimeoutMs())
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
            (listener, connectionState) ->
                LOG.info("Curator connection state changed to {}", connectionState));

    curator
        .getCuratorListenable()
        .addListener(
            (listener, curatorEvent) -> {
              if (curatorEvent.getType() == CuratorEventType.WATCHED
                  && curatorEvent.getWatchedEvent().getState()
                      == Watcher.Event.KeeperState.Expired) {
                LOG.warn("The ZK session has expired {}.", curatorEvent);

                if (!KaldbMetadataStore.persistentEphemeralModeEnabled()) {
                  new RuntimeHalterImpl().handleFatal(new Throwable("ZK session expired."));
                }
              }
            });

    curator.start();
    LOG.info(
        "Started curator server with the following config zkhost: {}, path prefix: {}, "
            + "connection timeout ms: {}, session timeout ms {} and retry policy {}",
        zkConfig.getZkConnectString(),
        zkConfig.getZkPathPrefix(),
        zkConfig.getZkConnectionTimeoutMs(),
        zkConfig.getZkSessionTimeoutMs(),
        retryPolicy);

    return AsyncCuratorFramework.wrap(curator);
  }
}
