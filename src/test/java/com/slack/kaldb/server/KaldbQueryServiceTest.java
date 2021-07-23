package com.slack.kaldb.server;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.TestKafkaServer.produceMessagesToKafka;
import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.TestKafkaServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class KaldbQueryServiceTest {

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private static SimpleMeterRegistry indexerMetricsRegistry;
  private static KaldbIndexer kaldbIndexer;

  private static KaldbServiceGrpc.KaldbServiceBlockingStub readServiceStub;
  private static Server indexingServer;
  private static Server queryServer;

  private static final String TEST_KAFKA_TOPIC = "test-topic";

  // Kafka producer creates only a partition 0 on first message. So, set the partition to 0 always.
  private static final int TEST_KAFKA_PARTITION = 0;

  private static final String KALDB_TEST_CLIENT = "kaldb-test-client";
  private static final String TEST_S3_BUCKET = "test-s3-bucket";
  private static TestKafkaServer kafkaServer;

  @BeforeClass
  // TODO: This test is very similar to KaldbIndexerTest - explore a TestRule based setup
  public static void initialize() throws Exception {
    kafkaServer = new TestKafkaServer();
    EphemeralKafkaBroker broker = kafkaServer.getBroker();
    assertThat(broker.isRunning()).isTrue();

    KaldbConfigs.KaldbConfig kaldbConfig =
        KaldbConfigUtil.makeKaldbConfig(
            "localhost:" + broker.getKafkaPort().get(),
            0,
            TEST_KAFKA_TOPIC,
            TEST_KAFKA_PARTITION,
            KALDB_TEST_CLIENT,
            TEST_S3_BUCKET,
            8081);
    KaldbConfig.initFromConfigObject(kaldbConfig);

    indexingServer = newIndexingServer(kaldbConfig);

    // wait at most 10 seconds to start before throwing an exception
    indexingServer.start().get(10, TimeUnit.SECONDS);

    // Produce messages to kafka, so the indexer can consume them.
    final Instant startTime =
        LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();
    produceMessagesToKafka(broker, startTime);
    Thread.sleep(1000); // Wait for consumer to finish consumption and roll over chunk.
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, indexerMetricsRegistry)).isEqualTo(100);

    // Don't respect the queryPort from the config - In case multiple tests run in parallel this
    // gives us a better chance to avoid port collisions
    queryServer = newQueryServer(kaldbConfig, (indexingServer.activeLocalPort() + 1));

    // wait at most 10 seconds to start before throwing an exception
    queryServer.start().get(10, TimeUnit.SECONDS);

    // We want to query the indexing server
    List<String> servers = new ArrayList<>();
    servers.add(String.format("gproto+http://127.0.0.1:%s/", indexingServer.activeLocalPort()));
    KaldbQueryService.servers = servers;

    readServiceStub =
        Clients.newClient(
            String.format("gproto+http://127.0.0.1:%s/", indexingServer.activeLocalPort() + 1),
            KaldbServiceGrpc.KaldbServiceBlockingStub.class);
  }

  private static Server newIndexingServer(KaldbConfigs.KaldbConfig kaldbConfig)
      throws InterruptedException {
    indexerMetricsRegistry = new SimpleMeterRegistry();
    ChunkManagerUtil<LogMessage> chunkManagerUtil =
        new ChunkManagerUtil<>(S3_MOCK_RULE, indexerMetricsRegistry, 10 * 1024 * 1024 * 1024L, 100);
    kaldbIndexer =
        new KaldbIndexer(
            chunkManagerUtil.chunkManager,
            KaldbIndexer.dataTransformerMap.get("api_log"),
            indexerMetricsRegistry);
    kaldbIndexer.start();
    Thread.sleep(1000); // Wait for consumer start.

    KaldbLocalSearcher<LogMessage> service =
        new KaldbLocalSearcher<>(kaldbIndexer.getChunkManager());

    return Server.builder()
        .http(kaldbConfig.getIndexerConfig().getServerPort())
        .verboseResponses(true)
        .service(GrpcService.builder().addService(service).build())
        .build();
  }

  public static Server newQueryServer(KaldbConfigs.KaldbConfig kaldbConfig, int queryPort) {
    KaldbQueryService service = new KaldbQueryService();
    return Server.builder()
        .http(queryPort)
        .verboseResponses(true)
        .service(GrpcService.builder().addService(service).build())
        .build();
  }

  @AfterClass
  public static void shutdownServer() throws Exception {
    if (kaldbIndexer != null) {
      kaldbIndexer.close();
    }
    if (kafkaServer != null) {
      kafkaServer.close();
    }
    if (indexerMetricsRegistry != null) {
      indexerMetricsRegistry.close();
    }
    if (indexingServer != null) {
      indexingServer.stop().join();
    }
    if (queryServer != null) {
      queryServer.stop().join();
    }
  }

  // TODO: Add a test to query multiple servers and aggregate their results.
  // TODO: Add a test where one of the servers times out/throws error.
  // TODO: Add a test for a non-existent server.
  // TODO: Add a case where the time out is hit even server responses are successful.

  @Test
  public void testSearch() {
    KaldbSearch.SearchResult searchResponse =
        readServiceStub.search(
            KaldbSearch.SearchRequest.newBuilder()
                .setIndexName(MessageUtil.TEST_INDEX_NAME)
                .setQueryString("*:*")
                .setStartTimeEpochMs(0L)
                .setEndTimeEpochMs(1601547099000L)
                .setHowMany(100)
                .setBucketCount(2)
                .build());

    assertThat(searchResponse.getHitsCount()).isEqualTo(100);
  }
}
