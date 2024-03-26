package com.slack.kaldb.preprocessor;

import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;

import com.google.common.util.concurrent.Service;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.core.KaldbMetadataTestUtils;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.MetricsUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreprocessorServiceIntegrationTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(PreprocessorServiceIntegrationTest.class);

  private TestKafkaServer kafkaServer;
  private TestingServer zkServer;

  @BeforeEach
  public void setUp() throws Exception {
    zkServer = new TestingServer();
    kafkaServer = new TestKafkaServer();
  }

  @AfterEach
  public void teardown() throws Exception {
    kafkaServer.close();
    zkServer.close();
  }

  @Test
  @Disabled("ZK reconnect support currently disabled")
  public void shouldHandleStreamError() throws Exception {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(zkServer.getConnectString())
            .setZkPathPrefix("test")
            .setZkSessionTimeoutMs(100)
            .setZkConnectionTimeoutMs(100)
            .setSleepBetweenRetriesMs(100)
            .build();
    AsyncCuratorFramework curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    DatasetMetadataStore datasetMetadataStore =
        new DatasetMetadataStore(curatorFramework, true, meterRegistry);

    KaldbConfigs.PreprocessorConfig.KafkaStreamConfig kafkaStreamConfig =
        KaldbConfigs.PreprocessorConfig.KafkaStreamConfig.newBuilder()
            .setApplicationId("applicationId")
            .setBootstrapServers(kafkaServer.getBroker().getBrokerList().get())
            .setNumStreamThreads(1)
            .setProcessingGuarantee("at_least_once")
            .build();
    KaldbConfigs.ServerConfig serverConfig =
        KaldbConfigs.ServerConfig.newBuilder()
            .setServerPort(8080)
            .setServerAddress("localhost")
            .build();
    KaldbConfigs.PreprocessorConfig preprocessorConfig =
        KaldbConfigs.PreprocessorConfig.newBuilder()
            .setKafkaStreamConfig(kafkaStreamConfig)
            .setServerConfig(serverConfig)
            .setPreprocessorInstanceCount(1)
            .setDataTransformer("api_log")
            .setRateLimiterMaxBurstSeconds(1)
            .addAllUpstreamTopics(List.of("foo"))
            .setDownstreamTopic("bar")
            .build();

    PreprocessorService preprocessorService =
        new PreprocessorService(datasetMetadataStore, preprocessorConfig, meterRegistry);

    datasetMetadataStore.createSync(
        new DatasetMetadata(
            "name",
            "owner",
            1,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("1"))),
            "name"));
    await().until(() -> datasetMetadataStore.listSync().size(), (size) -> size == 1);

    preprocessorService.startAsync();
    preprocessorService.awaitRunning(DEFAULT_START_STOP_DURATION);
    assertThat(MetricsUtil.getTimerCount(PreprocessorService.CONFIG_RELOAD_TIMER, meterRegistry))
        .isEqualTo(1);

    // restarting ZK should cause a stream application error due to missing source topics
    zkServer.restart();

    assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> zkServer.restart());

    await()
        .until(
            () -> preprocessorService.kafkaStreams.state(),
            KafkaStreams.State::hasCompletedShutdown);
    await().until(preprocessorService::state, (state) -> state.equals(Service.State.FAILED));
  }

  @Test
  public void shouldLoadConfigOnStartAndReloadOnMetadataChange() throws Exception {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(zkServer.getConnectString())
            .setZkPathPrefix("test")
            .setZkSessionTimeoutMs(30000)
            .setZkConnectionTimeoutMs(30000)
            .setSleepBetweenRetriesMs(30000)
            .build();
    AsyncCuratorFramework curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    DatasetMetadataStore datasetMetadataStore =
        new DatasetMetadataStore(curatorFramework, true, meterRegistry);

    KaldbConfigs.PreprocessorConfig.KafkaStreamConfig kafkaStreamConfig =
        KaldbConfigs.PreprocessorConfig.KafkaStreamConfig.newBuilder()
            .setApplicationId("applicationId")
            .setBootstrapServers(kafkaServer.getBroker().getBrokerList().get())
            .setNumStreamThreads(1)
            .build();
    KaldbConfigs.ServerConfig serverConfig =
        KaldbConfigs.ServerConfig.newBuilder()
            .setServerPort(8080)
            .setServerAddress("localhost")
            .build();
    KaldbConfigs.PreprocessorConfig preprocessorConfig =
        KaldbConfigs.PreprocessorConfig.newBuilder()
            .setKafkaStreamConfig(kafkaStreamConfig)
            .setServerConfig(serverConfig)
            .setPreprocessorInstanceCount(1)
            .setDataTransformer("api_log")
            .setRateLimiterMaxBurstSeconds(1)
            .build();

    PreprocessorService preprocessorService =
        new PreprocessorService(datasetMetadataStore, preprocessorConfig, meterRegistry);

    preprocessorService.startAsync();
    preprocessorService.awaitRunning(DEFAULT_START_STOP_DURATION);

    assertThat(MetricsUtil.getTimerCount(PreprocessorService.CONFIG_RELOAD_TIMER, meterRegistry))
        .isEqualTo(1);
    datasetMetadataStore.createSync(new DatasetMetadata("name", "owner", 0, List.of(), "name"));

    // wait for the cache to be updated
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);
    assertThat(MetricsUtil.getTimerCount(PreprocessorService.CONFIG_RELOAD_TIMER, meterRegistry))
        .isEqualTo(2);

    preprocessorService.stopAsync();
    preprocessorService.awaitTerminated();

    // close out the metadata stores
    datasetMetadataStore.close();
    curatorFramework.unwrap().close();
  }

  // Ignore flaky test. This test can be potentially merged with the above test.
  @Disabled
  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void shouldProcessMessageStartToFinish() throws Exception {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(zkServer.getConnectString())
            .setZkPathPrefix("test")
            .setZkSessionTimeoutMs(30000)
            .setZkConnectionTimeoutMs(30000)
            .setSleepBetweenRetriesMs(30000)
            .build();
    AsyncCuratorFramework curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    DatasetMetadataStore datasetMetadataStore =
        new DatasetMetadataStore(curatorFramework, true, meterRegistry);

    // initialize the downstream topic
    String downstreamTopic = "test-topic-out";
    kafkaServer.createTopicWithPartitions(downstreamTopic, 3);

    KaldbConfigs.PreprocessorConfig.KafkaStreamConfig kafkaStreamConfig =
        KaldbConfigs.PreprocessorConfig.KafkaStreamConfig.newBuilder()
            .setApplicationId("applicationId")
            .setBootstrapServers(kafkaServer.getBroker().getBrokerList().get())
            .setNumStreamThreads(1)
            .setProcessingGuarantee("at_least_once")
            .build();
    KaldbConfigs.ServerConfig serverConfig =
        KaldbConfigs.ServerConfig.newBuilder()
            .setServerPort(8080)
            .setServerAddress("localhost")
            .build();

    List<String> upstreamTopics = List.of("test-topic");
    KaldbConfigs.PreprocessorConfig preprocessorConfig =
        KaldbConfigs.PreprocessorConfig.newBuilder()
            .setKafkaStreamConfig(kafkaStreamConfig)
            .setServerConfig(serverConfig)
            .setPreprocessorInstanceCount(1)
            .addAllUpstreamTopics(upstreamTopics)
            .setDownstreamTopic(downstreamTopic)
            .setDataTransformer("api_log")
            .build();

    PreprocessorService preprocessorService =
        new PreprocessorService(datasetMetadataStore, preprocessorConfig, meterRegistry);

    preprocessorService.startAsync();
    preprocessorService.awaitRunning(DEFAULT_START_STOP_DURATION);

    assertThat(MetricsUtil.getTimerCount(PreprocessorService.CONFIG_RELOAD_TIMER, meterRegistry))
        .isEqualTo(1);

    // create a new service config with dummy data
    String serviceName = "testindex";
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            serviceName,
            "owner",
            100,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("3"))),
            serviceName);
    datasetMetadataStore.createSync(datasetMetadata);

    // wait for the cache to be updated
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);
    await()
        .until(
            () ->
                MetricsUtil.getTimerCount(PreprocessorService.CONFIG_RELOAD_TIMER, meterRegistry)
                    == 2);

    // update the service config with our desired configuration
    DatasetMetadata updatedDatasetMetadata =
        new DatasetMetadata(
            datasetMetadata.getName(),
            datasetMetadata.getOwner(),
            Long.MAX_VALUE,
            List.of(
                new DatasetPartitionMetadata(1, 10000, List.of("3")),
                new DatasetPartitionMetadata(10001, Long.MAX_VALUE, List.of("2"))),
            datasetMetadata.getName());
    datasetMetadataStore.updateSync(updatedDatasetMetadata);

    // wait for the cache to be updated
    await()
        .until(
            () ->
                MetricsUtil.getTimerCount(PreprocessorService.CONFIG_RELOAD_TIMER, meterRegistry)
                    == 3);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(1);
    assertThat(
            KaldbMetadataTestUtils.listSyncUncached(datasetMetadataStore)
                .get(0)
                .getThroughputBytes())
        .isEqualTo(Long.MAX_VALUE);

    // produce messages to upstream
    final Instant startTime = Instant.now();
    TestKafkaServer.produceMessagesToKafka(kafkaServer.getBroker(), startTime);

    // verify the message exist on the downstream
    Properties properties = kafkaServer.getBroker().consumerConfig();
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
    KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
    kafkaConsumer.subscribe(List.of(downstreamTopic));

    // wait until we see an offset of 100 on our target partition before we poll
    await()
        .until(
            () -> {
              @SuppressWarnings("OptionalGetWithoutIsPresent")
              Long partition2Offset =
                  ((Long)
                      kafkaConsumer
                          .endOffsets(List.of(new TopicPartition(downstreamTopic, 2)))
                          .values()
                          .stream()
                          .findFirst()
                          .get());
              LOG.debug("Current partition2Offset - {}", partition2Offset);
              return partition2Offset == 100;
            });

    // double check that only 100 records were fetched and all are on partition 2
    ConsumerRecords<String, byte[]> records =
        kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
    assertThat(records.count()).isEqualTo(100);
    records.forEach(record -> assertThat(record.partition()).isEqualTo(2));

    // close the kafka consumer used in the test
    kafkaConsumer.close();

    // close the preprocessor
    preprocessorService.stopAsync();
    preprocessorService.awaitTerminated();

    // close out the metadata stores
    datasetMetadataStore.close();
    curatorFramework.unwrap().close();
  }
}
