package com.slack.kaldb.writer.kafka;

import static java.lang.Integer.parseInt;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.chunk.ChunkRollOverException;
import com.slack.kaldb.util.RuntimeHalterImpl;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple wrapper class for Kafka Consumer. A kafka consumer is an infinite loop. So, it needs to
 * be run in a separate thread. Further, it is also important to shutdown the consumer cleanly so
 * that we can guarantee that the data is indexed only once.
 */
abstract class KaldbKafkaConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbKafkaConsumer.class);

  private final KafkaConsumer<String, byte[]> consumer;

  private final String kafkaTopic;
  private final int kafkaTopicPartition;
  private boolean stop;
  private final ListeningExecutorService kafkaConsumerService;
  private ListenableFuture<?> kafkaConsumerFuture;

  public KaldbKafkaConsumer(
      String kafkaTopic,
      String kafkaTopicPartitionStr,
      String kafkaBootStrapServers,
      String kafkaClientGroup,
      String enableKafkaAutoCommit,
      String kafkaAutoCommitInterval,
      String kafkaSessionTimeout) {

    stop = false;
    LOG.info(
        "Kafka params are: kafkaTopicName: {}, kafkaTopicPartition: {}, "
            + "kafkaBootstrapServers:{}, kafkaClientGroup: {}, kafkaAutoCommit:{}, "
            + "kafkaAutoCommitInterval: {}, kafkaSessionTimeout: {}",
        kafkaTopic,
        kafkaTopicPartitionStr,
        kafkaBootStrapServers,
        kafkaClientGroup,
        enableKafkaAutoCommit,
        kafkaAutoCommitInterval,
        kafkaSessionTimeout);

    if (kafkaTopicPartitionStr == null
        || kafkaTopic == null
        || kafkaTopic.isEmpty()
        || kafkaBootStrapServers == null
        || kafkaBootStrapServers.isEmpty()
        || kafkaClientGroup == null
        || kafkaClientGroup.isEmpty()
        || enableKafkaAutoCommit == null
        || enableKafkaAutoCommit.isEmpty()
        || kafkaAutoCommitInterval == null
        || kafkaAutoCommitInterval.isEmpty()
        || kafkaSessionTimeout == null
        || kafkaSessionTimeout.isEmpty()) {
      throw new IllegalArgumentException("Kafka params can't be null or empty.");
    }

    this.kafkaTopic = kafkaTopic;
    this.kafkaTopicPartition = parseInt(kafkaTopicPartitionStr);

    // Create kafka consumer
    this.consumer =
        buildKafkaConsumer(
            kafkaBootStrapServers,
            kafkaClientGroup,
            enableKafkaAutoCommit,
            kafkaAutoCommitInterval);

    if (kafkaTopicPartitionStr.isEmpty()) {
      LOG.info("Subscribing to kafka topic {}", kafkaTopic);
      consumer.subscribe(
          Collections.singletonList(kafkaTopic), new KafkaConsumerRebalanceListener());
    } else {
      // TODO: Consider using sticky consumer instead of assign?
      LOG.info(
          "Assigned to kafka topic {} and partition {}", this.kafkaTopic, this.kafkaTopicPartition);
      consumer.assign(
          Collections.singletonList(new TopicPartition(this.kafkaTopic, kafkaTopicPartition)));
    }

    kafkaConsumerService =
        MoreExecutors.listeningDecorator(
            MoreExecutors.getExitingExecutorService(
                (ThreadPoolExecutor) Executors.newFixedThreadPool(1), 2, TimeUnit.SECONDS));
  }

  private KafkaConsumer<String, byte[]> buildKafkaConsumer(
      String kafkaBootStrapServers,
      String kafkaClientGroup,
      String enableKafkaAutoCommit,
      String kafkaAutoCommitInterval) {

    Properties props = new Properties();
    props.put("bootstrap.servers", kafkaBootStrapServers);
    props.put("group.id", kafkaClientGroup);
    // TODO: Consider committing manual consumer offset?
    props.put("enable.auto.commit", enableKafkaAutoCommit);
    props.put("auto.commit.interval.ms", kafkaAutoCommitInterval);
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    // TODO: Using ByteArrayDeserializer since it's most primitive and performant. Replace it if
    // not.
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    return new KafkaConsumer<>(props);
  }

  public ListenableFuture<?> start() {
    LOG.info("Starting kafka consumer.");

    // We can only have one consumer per class. So, throw an exception if it's called twice.
    if (kafkaConsumerFuture != null) {
      throw new IllegalStateException("Start shouldn't be called twice.");
    }

    kafkaConsumerFuture = kafkaConsumerService.submit(this::run);
    return kafkaConsumerFuture;
  }

  private void run() {
    while (true) {
      try {
        if (stop) {
          close();
          break;
        }

        long kafkaPollTimeoutMs = 100;
        consumeMessages(kafkaPollTimeoutMs);
      } catch (RejectedExecutionException e) {
        // This case shouldn't happen since there is only one thread queuing tasks here and we check
        // that the queue is empty before polling kafka.
        LOG.error("Rejected execution shouldn't happen ", e);
      } catch (ChunkRollOverException | IOException e) {
        // Once we hit these exceptions, we likely have an issue related to storage. So, terminate
        // the program, since consuming more messages from Kafka would only make the issue worse.
        LOG.error("FATAL: Encountered an unrecoverable storage exception.", e);
        new RuntimeHalterImpl().handleFatal(e);
      } catch (Exception e) {
        LOG.error("FATAL: Unhandled exception ", e);
        new RuntimeHalterImpl().handleFatal(e);
      }
    }
  }

  protected KafkaConsumer<String, byte[]> getConsumer() {
    return consumer;
  }

  protected TopicPartition getTopicPartition() {
    return new TopicPartition(this.kafkaTopic, this.kafkaTopicPartition);
  }

  abstract void consumeMessages(long kafkaPollTimeoutMs) throws IOException;

  private void close() {
    if (stop && consumer != null) {
      LOG.info("Closing kafka consumer.");
      consumer.close();
      kafkaConsumerService.shutdown();
      LOG.info("Closed kafka consumer.");
    }
  }

  public ListenableFuture<?> triggerShutdown() {
    stop = true;
    return kafkaConsumerFuture;
  }

  public boolean isShutdown() {
    return kafkaConsumerService.isShutdown();
  }
}
