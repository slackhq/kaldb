package com.slack.kaldb.server;

import static com.linecorp.armeria.common.HttpStatus.INTERNAL_SERVER_ERROR;
import static com.linecorp.armeria.common.HttpStatus.OK;
import static com.linecorp.armeria.common.HttpStatus.TOO_MANY_REQUESTS;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.google.protobuf.InvalidProtocolBufferException;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.slack.kaldb.bulkIngestApi.BulkIngestApi;
import com.slack.kaldb.bulkIngestApi.BulkIngestKafkaProducer;
import com.slack.kaldb.bulkIngestApi.BulkIngestResponse;
import com.slack.kaldb.bulkIngestApi.DatasetRateLimitingService;
import com.slack.kaldb.bulkIngestApi.opensearch.BulkApiRequestParser;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata;
import com.slack.kaldb.preprocessor.PreprocessorRateLimiter;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.schema.Schema;
import com.slack.kaldb.testlib.MetricsUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import com.slack.kaldb.util.JsonUtil;
import com.slack.kaldb.util.TestingZKServer;
import com.slack.service.murron.trace.Trace;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkIngestApiTest {
  private static final Logger LOG = LoggerFactory.getLogger(BulkIngestApi.class);
  private static PrometheusMeterRegistry meterRegistry;
  private static AsyncCuratorFramework curatorFramework;
  private static KaldbConfigs.PreprocessorConfig preprocessorConfig;
  private static DatasetMetadataStore datasetMetadataStore;
  private static TestingServer zkServer;
  private static TestKafkaServer kafkaServer;
  private BulkIngestApi bulkApi;

  private BulkIngestKafkaProducer bulkIngestKafkaProducer;
  private DatasetRateLimitingService datasetRateLimitingService;

  static String INDEX_NAME = "testindex";

  private static String DOWNSTREAM_TOPIC = "test-topic-out";

  @BeforeEach
  public void bootstrapCluster() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    zkServer = TestingZKServer.createTestingServer();
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(zkServer.getConnectString())
            .setZkPathPrefix("testZK")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();
    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);

    kafkaServer = new TestKafkaServer();
    kafkaServer.createTopicWithPartitions(DOWNSTREAM_TOPIC, 3);

    KaldbConfigs.ServerConfig serverConfig =
        KaldbConfigs.ServerConfig.newBuilder()
            .setServerPort(8080)
            .setServerAddress("localhost")
            .build();
    KaldbConfigs.KafkaConfig kafkaConfig =
        KaldbConfigs.KafkaConfig.newBuilder()
            .setKafkaBootStrapServers(kafkaServer.getBroker().getBrokerList().get())
            .setKafkaTopic(DOWNSTREAM_TOPIC)
            .build();
    preprocessorConfig =
        KaldbConfigs.PreprocessorConfig.newBuilder()
            .setKafkaConfig(kafkaConfig)
            .setUseBulkApi(true)
            .setServerConfig(serverConfig)
            .setPreprocessorInstanceCount(1)
            .setRateLimiterMaxBurstSeconds(1)
            .build();

    datasetMetadataStore = new DatasetMetadataStore(curatorFramework, true, meterRegistry);
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            INDEX_NAME,
            "owner",
            1,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("0"))),
            INDEX_NAME);
    // Create an entry while init. Update the entry on every test run
    datasetMetadataStore.createSync(datasetMetadata);

    datasetRateLimitingService =
        new DatasetRateLimitingService(datasetMetadataStore, preprocessorConfig, meterRegistry);
    bulkIngestKafkaProducer =
        new BulkIngestKafkaProducer(datasetMetadataStore, preprocessorConfig, meterRegistry);

    datasetRateLimitingService.startAsync();
    bulkIngestKafkaProducer.startAsync();

    datasetRateLimitingService.awaitRunning(DEFAULT_START_STOP_DURATION);
    bulkIngestKafkaProducer.awaitRunning(DEFAULT_START_STOP_DURATION);

    bulkApi =
        new BulkIngestApi(
            bulkIngestKafkaProducer,
            datasetRateLimitingService,
            meterRegistry,
            400,
            Schema.IngestSchema.newBuilder().build());
  }

  // I looked at making this a @BeforeEach. it's possible if you annotate a test with a @Tag and
  // pass throughputBytes.
  // However, decided not to go with that because it involved hardcoding the throughput bytes
  // when defining the test. We need it to be dynamic based on the size of the docs
  public void updateDatasetThroughput(int throughputBytes) {
    double timerCount =
        MetricsUtil.getTimerCount(
            DatasetRateLimitingService.RATE_LIMIT_RELOAD_TIMER, meterRegistry);

    // dataset metadata already exists. Update with the throughput value
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            INDEX_NAME,
            "owner",
            throughputBytes,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("0"))),
            INDEX_NAME);
    datasetMetadataStore.updateSync(datasetMetadata);

    // Need to wait until the rate limit has been loaded
    await()
        .until(
            () ->
                MetricsUtil.getTimerCount(
                        DatasetRateLimitingService.RATE_LIMIT_RELOAD_TIMER, meterRegistry)
                    > timerCount);
  }

  @AfterEach
  // Every test manually calls setup() since we need to pass a param while creating the dataset
  // Instead of calling stop from every test and ensuring it's part of a finally block we just call
  // the shutdown code with the @AfterEach annotation
  public void shutdownOpenSearchAPI() throws Exception {
    if (datasetRateLimitingService != null) {
      datasetRateLimitingService.stopAsync();
      datasetRateLimitingService.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    if (bulkIngestKafkaProducer != null) {
      bulkIngestKafkaProducer.stopAsync();
      bulkIngestKafkaProducer.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    kafkaServer.close();
    curatorFramework.unwrap().close();
    zkServer.close();
    meterRegistry.close();
  }

  public KafkaConsumer getTestKafkaConsumer() {
    // used to verify the message exist on the downstream topic
    Properties properties = kafkaServer.getBroker().consumerConfig();
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
    properties.put("isolation.level", "read_committed");
    KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
    kafkaConsumer.subscribe(List.of(DOWNSTREAM_TOPIC));
    return kafkaConsumer;
  }

  @Test
  public void testBulkApiBasic() throws Exception {
    String request1 =
        """
                { "index": {"_index": "testindex", "_id": "1"} }
                { "field1" : "value1" }
                """;
    // use the way we calculate the throughput in the rate limiter to get the exact bytes
    Map<String, List<Trace.Span>> docs =
        BulkApiRequestParser.parseRequest(
            request1.getBytes(StandardCharsets.UTF_8), Schema.IngestSchema.newBuilder().build());
    int limit = PreprocessorRateLimiter.getSpanBytes(docs.get("testindex"));
    // for some reason if we pass the exact limit, the rate limiter doesn't work as expected
    updateDatasetThroughput(limit / 2);

    // test with empty causes a parse exception
    AggregatedHttpResponse response = bulkApi.addDocument("{}\n").aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(false);
    assertThat(response.status().code()).isEqualTo(INTERNAL_SERVER_ERROR.code());
    BulkIngestResponse responseObj =
        JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(0);
    assertThat(responseObj.failedDocs()).isEqualTo(0);

    // test with request1 twice. first one should succeed, second one will fail because of rate
    // limiter
    AggregatedHttpResponse httpResponse = bulkApi.addDocument(request1).aggregate().join();
    assertThat(httpResponse.status().isSuccess()).isEqualTo(true);
    assertThat(httpResponse.status().code()).isEqualTo(OK.code());
    try {
      BulkIngestResponse httpResponseObj =
          JsonUtil.read(httpResponse.contentUtf8(), BulkIngestResponse.class);
      assertThat(httpResponseObj.totalDocs()).isEqualTo(1);
      assertThat(httpResponseObj.failedDocs()).isEqualTo(0);
    } catch (IOException e) {
      fail("", e);
    }

    httpResponse = bulkApi.addDocument(request1).aggregate().join();
    assertThat(httpResponse.status().isSuccess()).isEqualTo(false);
    assertThat(httpResponse.status().code()).isEqualTo(400);
    try {
      BulkIngestResponse httpResponseObj =
          JsonUtil.read(httpResponse.contentUtf8(), BulkIngestResponse.class);
      assertThat(httpResponseObj.totalDocs()).isEqualTo(0);
      assertThat(httpResponseObj.failedDocs()).isEqualTo(0);
      assertThat(httpResponseObj.errorMsg()).isEqualTo("rate limit exceeded");
    } catch (IOException e) {
      fail("", e);
    }

    // test with multiple indexes
    String request2 =
        """
                { "index": {"_index": "testindex1", "_id": "1"} }
                { "field1" : "value1" }
                { "index": {"_index": "testindex2", "_id": "1"} }
                { "field1" : "value1" }
                """;
    response = bulkApi.addDocument(request2).aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(false);
    assertThat(response.status().code()).isEqualTo(INTERNAL_SERVER_ERROR.code());
    responseObj = JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(0);
    assertThat(responseObj.failedDocs()).isEqualTo(0);
    assertThat(responseObj.errorMsg()).isEqualTo("request must contain only 1 unique index");

    BulkIngestApi bulkApi2 =
        new BulkIngestApi(
            bulkIngestKafkaProducer,
            datasetRateLimitingService,
            meterRegistry,
            TOO_MANY_REQUESTS.code(),
            Schema.IngestSchema.newBuilder().build());
    httpResponse = bulkApi2.addDocument(request1).aggregate().join();
    assertThat(httpResponse.status().isSuccess()).isEqualTo(false);
    assertThat(httpResponse.status().code()).isEqualTo(TOO_MANY_REQUESTS.code());
    try {
      BulkIngestResponse httpResponseObj =
          JsonUtil.read(httpResponse.contentUtf8(), BulkIngestResponse.class);
      assertThat(httpResponseObj.totalDocs()).isEqualTo(0);
      assertThat(httpResponseObj.failedDocs()).isEqualTo(0);
      assertThat(httpResponseObj.errorMsg()).isEqualTo("rate limit exceeded");
    } catch (IOException e) {
      fail("", e);
    }
  }

  @Test
  public void testDocumentInKafkaSimple() throws Exception {
    String request1 =
        """
                    { "index": {"_index": "testindex", "_id": "1"} }
                    { "field1" : "value1" },
                    { "index": {"_index": "testindex", "_id": "2"} }
                    { "field1" : "value2" }
                    """;
    updateDatasetThroughput(request1.getBytes(StandardCharsets.UTF_8).length);

    KafkaConsumer kafkaConsumer = getTestKafkaConsumer();

    AggregatedHttpResponse response = bulkApi.addDocument(request1).aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(true);
    assertThat(response.status().code()).isEqualTo(OK.code());
    BulkIngestResponse responseObj =
        JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(2);
    assertThat(responseObj.failedDocs()).isEqualTo(0);

    // kafka transaction adds a "control batch" record at the end of the transaction so the offset
    // will always be n+1
    validateOffset(kafkaConsumer, 3);
    ConsumerRecords<String, byte[]> records =
        kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

    assertThat(records.count()).isEqualTo(2);
    assertThat(records)
        .anyMatch(
            record ->
                TraceSpanParserSilenceError(record.value()).getId().toStringUtf8().equals("1"));
    assertThat(records)
        .anyMatch(
            record ->
                TraceSpanParserSilenceError(record.value()).getId().toStringUtf8().equals("2"));

    // close the kafka consumer used in the test
    kafkaConsumer.close();
  }

  public void validateOffset(KafkaConsumer kafkaConsumer, long expectedOffset) {
    await()
        .until(
            () -> {
              @SuppressWarnings("OptionalGetWithoutIsPresent")
              long partitionOffset =
                  (Long)
                      kafkaConsumer
                          .endOffsets(List.of(new TopicPartition(DOWNSTREAM_TOPIC, 0)))
                          .values()
                          .stream()
                          .findFirst()
                          .get();
              LOG.debug(
                  "Current partitionOffset - {}. expecting offset to be - {}",
                  partitionOffset,
                  expectedOffset);
              return partitionOffset == expectedOffset;
            });
  }

  private static Trace.Span TraceSpanParserSilenceError(byte[] data) {
    try {
      return Trace.Span.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      return Trace.Span.newBuilder().build();
    }
  }
}
