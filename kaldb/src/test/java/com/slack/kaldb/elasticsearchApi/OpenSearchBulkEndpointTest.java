package com.slack.kaldb.elasticsearchApi;

import static com.linecorp.armeria.common.HttpStatus.INTERNAL_SERVER_ERROR;
import static com.linecorp.armeria.common.HttpStatus.OK;
import static com.linecorp.armeria.common.HttpStatus.TOO_MANY_REQUESTS;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.google.protobuf.InvalidProtocolBufferException;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata;
import com.slack.kaldb.preprocessor.PreprocessorRateLimiter;
import com.slack.kaldb.preprocessor.ingest.OpenSearchBulkApiRequestParser;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.server.OpenSearchBulkIngestApi;
import com.slack.kaldb.testlib.TestKafkaServer;
import com.slack.kaldb.util.JsonUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opensearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchBulkEndpointTest {

  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchBulkEndpointTest.class);

  private static PrometheusMeterRegistry meterRegistry;
  private static AsyncCuratorFramework curatorFramework;
  private static KaldbConfigs.PreprocessorConfig preprocessorConfig;
  private static DatasetMetadataStore datasetMetadataStore;
  private static TestingServer zkServer;
  private static TestKafkaServer kafkaServer;
  private OpenSearchBulkIngestApi openSearchBulkAPI;

  static String INDEX_NAME = "testindex";

  private static String DOWNSTREAM_TOPIC = "test-topic-out";

  @BeforeAll
  public static void beforeClass() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    zkServer = new TestingServer();
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
    preprocessorConfig =
        KaldbConfigs.PreprocessorConfig.newBuilder()
            .setBootstrapServers(kafkaServer.getBroker().getBrokerList().get())
            .setUseBulkApi(true)
            .setServerConfig(serverConfig)
            .setPreprocessorInstanceCount(1)
            .setRateLimiterMaxBurstSeconds(1)
            .setDownstreamTopic(DOWNSTREAM_TOPIC)
            .build();

    datasetMetadataStore = new DatasetMetadataStore(curatorFramework, true);
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            INDEX_NAME,
            "owner",
            1,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("0"))),
            INDEX_NAME);
    // Create an entry while init. Update the entry on every test run
    datasetMetadataStore.createSync(datasetMetadata);
  }

  @AfterAll
  public static void shutdown() throws Exception {
    kafkaServer.close();
    curatorFramework.unwrap().close();
    zkServer.close();
    meterRegistry.close();
  }

  // I looked at making this a @BeforeEach. it's possible if you annotate a test with a @Tag and
  // pass throughputBytes.
  // However, decided not to go with that because it involved hardcoding the throughput bytes
  // when defining the test. We need it to be dynamic based on the size of the docs
  public void setup(int throughputBytes) throws Exception {

    // dataset metadata already exists. Update with the throughput value
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            INDEX_NAME,
            "owner",
            throughputBytes,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("0"))),
            INDEX_NAME);
    datasetMetadataStore.updateSync(datasetMetadata);

    openSearchBulkAPI =
        new OpenSearchBulkIngestApi(datasetMetadataStore, preprocessorConfig, meterRegistry, false);

    openSearchBulkAPI.startAsync();
    openSearchBulkAPI.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  @AfterEach
  // Every test manually calls setup() since we need to pass a param while creating the dataset
  // Instead of calling stop from every test and ensuring it's part of a finally block we just call
  // the shutdown code with the @AfterEach annotation
  public void shutdownOpenSearchAPI() throws TimeoutException {
    if (openSearchBulkAPI != null) {
      openSearchBulkAPI.stopAsync();
      openSearchBulkAPI.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
  }

  @Test
  public void testBulkApiBasic() throws Exception {
    String request1 =
        """
                { "index": {"_index": "testindex", "_id": "1"} }
                { "field1" : "value1" }
                """;
    // get num bytes that can be used to create the dataset. When we make 2 successive calls the
    // second one should fail
    List<IndexRequest> indexRequests = OpenSearchBulkApiRequestParser.parseBulkRequest(request1);
    Map<String, List<Trace.Span>> indexDocs =
        OpenSearchBulkApiRequestParser.convertIndexRequestToTraceFormat(indexRequests);
    assertThat(indexDocs.keySet().size()).isEqualTo(1);
    assertThat(indexDocs.get("testindex").size()).isEqualTo(1);
    assertThat(indexDocs.get("testindex").get(0).getId().toStringUtf8()).isEqualTo("1");
    int throughputBytes = PreprocessorRateLimiter.getSpanBytes(indexDocs.get("testindex"));
    setup(throughputBytes);

    // test with empty causes a parse exception
    AggregatedHttpResponse response = openSearchBulkAPI.addDocument("{}\n").aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(false);
    assertThat(response.status().code()).isEqualTo(INTERNAL_SERVER_ERROR.code());
    BulkIngestResponse responseObj =
        JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(0);
    assertThat(responseObj.failedDocs()).isEqualTo(0);

    // test with request1 twice. first one should succeed, second one will fail because of rate
    // limiter
    response = openSearchBulkAPI.addDocument(request1).aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(true);
    assertThat(response.status().code()).isEqualTo(OK.code());
    responseObj = JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(1);
    assertThat(responseObj.failedDocs()).isEqualTo(0);

    response = openSearchBulkAPI.addDocument(request1).aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(false);
    assertThat(response.status().code()).isEqualTo(TOO_MANY_REQUESTS.code());
    responseObj = JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(0);
    assertThat(responseObj.failedDocs()).isEqualTo(0);
    assertThat(responseObj.errorMsg()).isEqualTo("rate limit exceeded");

    // test with multiple indexes
    String request2 =
        """
                { "index": {"_index": "testindex1", "_id": "1"} }
                { "field1" : "value1" }
                { "index": {"_index": "testindex2", "_id": "1"} }
                { "field1" : "value1" }
                """;
    response = openSearchBulkAPI.addDocument(request2).aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(false);
    assertThat(response.status().code()).isEqualTo(INTERNAL_SERVER_ERROR.code());
    responseObj = JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(0);
    assertThat(responseObj.failedDocs()).isEqualTo(0);
    assertThat(responseObj.errorMsg()).isEqualTo("request must contain only 1 unique index");
  }

  @Test
  public void testDocumentInKafka() throws Exception {
    String request1 =
        """
                    { "index": {"_index": "testindex", "_id": "1"} }
                    { "field1" : "value1" },
                    { "index": {"_index": "testindex", "_id": "2"} }
                    { "field1" : "value2" }
                    """;
    List<IndexRequest> indexRequests = OpenSearchBulkApiRequestParser.parseBulkRequest(request1);
    Map<String, List<Trace.Span>> indexDocs =
        OpenSearchBulkApiRequestParser.convertIndexRequestToTraceFormat(indexRequests);
    assertThat(indexDocs.keySet().size()).isEqualTo(1);
    assertThat(indexDocs.get("testindex").size()).isEqualTo(2);
    int throughputBytes = PreprocessorRateLimiter.getSpanBytes(indexDocs.get("testindex"));
    setup(throughputBytes);

    // used to verify the message exist on the downstream topic
    Properties properties = kafkaServer.getBroker().consumerConfig();
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
    KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
    kafkaConsumer.subscribe(List.of(DOWNSTREAM_TOPIC));

    AggregatedHttpResponse response = openSearchBulkAPI.addDocument(request1).aggregate().join();
    assertThat(response.status().isSuccess()).isEqualTo(true);
    assertThat(response.status().code()).isEqualTo(OK.code());
    BulkIngestResponse responseObj =
        JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseObj.totalDocs()).isEqualTo(2);
    assertThat(responseObj.failedDocs()).isEqualTo(0);

    // kafka transaction adds a "control batch" record at the end of the transaction so the offset
    // will always be n+1
    await()
        .until(
            () -> {
              @SuppressWarnings("OptionalGetWithoutIsPresent")
              Long partitionOffset =
                  ((Long)
                      kafkaConsumer
                          .endOffsets(List.of(new TopicPartition(DOWNSTREAM_TOPIC, 0)))
                          .values()
                          .stream()
                          .findFirst()
                          .get());
              LOG.debug("Current partitionOffset - {}", partitionOffset);
              return partitionOffset == 3;
            });
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

  private static Trace.Span TraceSpanParserSilenceError(byte[] data) {
    try {
      return Trace.Span.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      return Trace.Span.newBuilder().build();
    }
  }
}