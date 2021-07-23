package com.slack.kaldb.writer.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.with;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.google.common.util.concurrent.ListenableFuture;
import com.slack.kaldb.testlib.TestKafkaServer;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Enclosed.class)
public class KaldbKafkaConsumerTest {
  // TODO: Add a test to make sure catchup is working as expected.
  private static final Logger LOG = LoggerFactory.getLogger(KaldbKafkaConsumerTest.class);

  static class TestKaldbKafkaConsumer extends KaldbKafkaConsumer {
    private int recordCount = 0;

    public TestKaldbKafkaConsumer(String kafkaBootStrapServers, String testKafkaClientGroup) {
      super(
          TestKafkaServer.TEST_KAFKA_TOPIC,
          String.valueOf(TestKafkaServer.TEST_KAFKA_PARTITION),
          kafkaBootStrapServers,
          testKafkaClientGroup,
          "true",
          "5000",
          "5000");
    }

    @Override
    void consumeMessages(long kafkaPollTimeoutMs) {
      ConsumerRecords<String, byte[]> records =
          getConsumer().poll(Duration.ofMillis(kafkaPollTimeoutMs));
      recordCount += records.count();
    }

    public int getRecordCount() {
      return recordCount;
    }
  }

  public static class BasicTests {
    private TestKafkaServer kafkaServer;
    private TestKaldbKafkaConsumer testConsumer;

    @Before
    public void setUp() throws Exception {
      kafkaServer = new TestKafkaServer();
      testConsumer =
          new TestKaldbKafkaConsumer(kafkaServer.getBroker().getBrokerList().get(), "basic_tests");
    }

    @After
    public void tearDown()
        throws ExecutionException, InterruptedException, TimeoutException, NoSuchFieldException,
            IllegalAccessException, IOException {
      ListenableFuture<?> future = testConsumer.triggerShutdown();
      future.get(1, TimeUnit.SECONDS);
      assertThat(future.isDone()).isTrue();

      // Close server after consumer done.
      kafkaServer.close();
    }

    @Test
    public void kafkaConsumerStartupShutdown() throws Exception {
      EphemeralKafkaBroker broker = kafkaServer.getBroker();
      assertThat(broker.isRunning()).isTrue();
      final Instant startTime =
          LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

      ExecutorService indexerExecutor = Executors.newSingleThreadExecutor();
      indexerExecutor.submit(testConsumer::start);
      Thread.sleep(1000); // Wait for consumer start.

      TestKafkaServer.produceMessagesToKafka(broker, startTime);
      Thread.sleep(500); // Wait for consumption.

      assertThat(testConsumer.getRecordCount()).isEqualTo(100);
    }
  }

  public static class TimeoutTests {
    private TestKafkaServer kafkaServer;
    private TestKaldbKafkaConsumer testConsumer;

    @Before
    public void setUp() throws Exception {
      kafkaServer = new TestKafkaServer();
      testConsumer =
          new TestKaldbKafkaConsumer(
              kafkaServer.getBroker().getBrokerList().get(), "timeout_tests");
    }

    @After
    public void tearDown() throws ExecutionException, InterruptedException, TimeoutException {
      // KafkaConsumer.DEFAULT_CLOSE_TIMEOUT_MS is 30 * 1000, so wait that plus a little extra
      with()
          .pollInterval(1, TimeUnit.SECONDS)
          .await()
          .atMost(45, TimeUnit.SECONDS)
          .until(() -> testConsumer.isShutdown());
    }

    @Test(expected = TimeoutException.class)
    public void kafkaConsumerShutdownTimeout() throws Exception {
      EphemeralKafkaBroker broker = kafkaServer.getBroker();
      assertThat(broker.isRunning()).isTrue();
      final Instant startTime =
          LocalDateTime.of(2020, 10, 1, 10, 10, 0).atZone(ZoneOffset.UTC).toInstant();

      ExecutorService indexerExecutor = Executors.newSingleThreadExecutor();
      indexerExecutor.submit(testConsumer::start);
      Thread.sleep(1000); // Wait for consumer start.

      TestKafkaServer.produceMessagesToKafka(broker, startTime);
      Thread.sleep(500); // Wait for consumption.

      assertThat(testConsumer.getRecordCount()).isEqualTo(100);

      // Closing server before consumer should lead to time out.
      kafkaServer.close();
      ListenableFuture<?> future = testConsumer.triggerShutdown();
      future.get(1, TimeUnit.SECONDS);
    }
  }
}
