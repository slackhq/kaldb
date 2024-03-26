package com.slack.kaldb.bulkIngestApi;

import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import brave.Tracing;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.MetricsUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import com.slack.service.murron.trace.Trace;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BulkIngestKafkaProducerTest {
  private static final Logger LOG = LoggerFactory.getLogger(BulkIngestKafkaProducerTest.class);
  private static PrometheusMeterRegistry meterRegistry;
  private static AsyncCuratorFramework curatorFramework;
  private static KaldbConfigs.PreprocessorConfig preprocessorConfig;
  private static DatasetMetadataStore datasetMetadataStore;
  private static TestingServer zkServer;
  private static TestKafkaServer kafkaServer;

  private BulkIngestKafkaProducer bulkIngestKafkaProducer;

  static String INDEX_NAME = "testtransactionindex";

  private static String DOWNSTREAM_TOPIC = "test-transaction-topic-out";

  @BeforeEach
  public void bootstrapCluster() throws Exception {
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
    kafkaServer.createTopicWithPartitions(DOWNSTREAM_TOPIC, 5);

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

    bulkIngestKafkaProducer =
        new BulkIngestKafkaProducer(datasetMetadataStore, preprocessorConfig, meterRegistry);
    bulkIngestKafkaProducer.startAsync();
    bulkIngestKafkaProducer.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void testKafkaCanRestartOnError() {
    Trace.Span doc1 = spy(Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("error1")).build());
    Map<String, List<Trace.Span>> indexDocsError = Map.of(INDEX_NAME, List.of(doc1));

    // this isn't exactly where the kafka timeout exception is thrown from, but will get trapped in
    // the same manner
    when(doc1.toByteArray()).thenThrow(TimeoutException.class);

    assertThat(
            MetricsUtil.getTimerCount(BulkIngestKafkaProducer.KAFKA_RESTART_COUNTER, meterRegistry))
        .isEqualTo(0);

    BulkIngestRequest request = bulkIngestKafkaProducer.submitRequest(indexDocsError);
    AtomicReference<BulkIngestResponse> response = new AtomicReference<>();

    // need a consumer thread for reading synchronous queue
    Thread.ofVirtual()
        .start(
            () -> {
              try {
                response.set(request.getResponse());
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });

    await().until(() -> response.get() != null);
    assertThat(
            MetricsUtil.getTimerCount(BulkIngestKafkaProducer.KAFKA_RESTART_COUNTER, meterRegistry))
        .isEqualTo(1);
    assertThat(response.get().failedDocs()).isEqualTo(1);

    // try to put a doc successfully after restarting
    Trace.Span doc2 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("noerror")).build();
    Map<String, List<Trace.Span>> indexDocsNoError = Map.of(INDEX_NAME, List.of(doc2));

    BulkIngestRequest requestOk = bulkIngestKafkaProducer.submitRequest(indexDocsNoError);
    AtomicReference<BulkIngestResponse> responseOk = new AtomicReference<>();

    // need a consumer thread for reading synchronous queue
    Thread.ofVirtual()
        .start(
            () -> {
              try {
                responseOk.set(requestOk.getResponse());
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });

    await().until(() -> responseOk.get() != null);
    assertThat(responseOk.get().totalDocs()).isEqualTo(1);
    assertThat(responseOk.get().failedDocs()).isEqualTo(0);

    // restart should still be at one
    assertThat(
            MetricsUtil.getTimerCount(BulkIngestKafkaProducer.KAFKA_RESTART_COUNTER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void testDocumentInKafkaTransactionError() throws Exception {
    KafkaConsumer kafkaConsumer = getTestKafkaConsumer();

    // we want to inject a failure in the second doc and test if the abort transaction works and we
    // don't index the first document
    Trace.Span doc1 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("error1")).build();
    Trace.Span doc2 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("error2")).build();
    Trace.Span doc3 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("error3")).build();
    Trace.Span doc4 = spy(Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("error4")).build());
    when(doc4.toByteArray()).thenThrow(new RuntimeException("exception"));
    Trace.Span doc5 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("error5")).build();

    Map<String, List<Trace.Span>> indexDocs =
        Map.of(INDEX_NAME, List.of(doc1, doc2, doc3, doc4, doc5));

    BulkIngestRequest request1 = new BulkIngestRequest(indexDocs);
    Thread.ofVirtual()
        .start(
            () -> {
              try {
                // because of the synchronous queue, we need someone consuming the response before
                // we attempt to set it
                request1.getResponse();
              } catch (InterruptedException ignored) {
              }
            });
    BulkIngestResponse responseObj =
        (BulkIngestResponse)
            bulkIngestKafkaProducer
                .produceDocumentsAndCommit(List.of(request1))
                .values()
                .toArray()[0];
    assertThat(responseObj.totalDocs()).isEqualTo(0);
    assertThat(responseObj.failedDocs()).isEqualTo(5);
    assertThat(responseObj.errorMsg()).isNotNull();

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
                  "Current partitionOffset - {}. expecting offset to be less than 5",
                  partitionOffset);
              return partitionOffset > 0 && partitionOffset < 5;
            });

    ConsumerRecords<String, byte[]> records =
        kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

    assertThat(records.count()).isEqualTo(0);

    long currentPartitionOffset =
        (Long)
            kafkaConsumer
                .endOffsets(List.of(new TopicPartition(DOWNSTREAM_TOPIC, 0)))
                .values()
                .stream()
                .findFirst()
                .get();

    Trace.Span doc6 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("no_error6")).build();
    Trace.Span doc7 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("no_error7")).build();
    Trace.Span doc8 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("no_error8")).build();
    Trace.Span doc9 =
        spy(Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("no_error9")).build());
    Trace.Span doc10 = Trace.Span.newBuilder().setId(ByteString.copyFromUtf8("no_error10")).build();

    indexDocs = Map.of(INDEX_NAME, List.of(doc6, doc7, doc8, doc9, doc10));

    BulkIngestRequest request2 = new BulkIngestRequest(indexDocs);
    Thread.ofVirtual()
        .start(
            () -> {
              try {
                // because of the synchronous queue, we need someone consuming the response before
                // we attempt to set it
                request2.getResponse();
              } catch (InterruptedException ignored) {
              }
            });
    responseObj =
        (BulkIngestResponse)
            bulkIngestKafkaProducer
                .produceDocumentsAndCommit(List.of(request2))
                .values()
                .toArray()[0];
    assertThat(responseObj.totalDocs()).isEqualTo(5);
    assertThat(responseObj.failedDocs()).isEqualTo(0);
    assertThat(responseObj.errorMsg()).isNotNull();

    // 5 docs. 1 control batch. initial offset was 1 after the first failed batch
    validateOffset(kafkaConsumer, currentPartitionOffset + 5 + 1);
    records = kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

    assertThat(records.count()).isEqualTo(5);
    records.forEach(
        record ->
            LOG.info(
                "Trace= + " + TraceSpanParserSilenceError(record.value()).getId().toStringUtf8()));

    // close the kafka consumer used in the test
    kafkaConsumer.close();
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
