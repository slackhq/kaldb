package com.slack.kaldb.server;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.slack.kaldb.proto.config.KaldbConfigs;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AbortTransactionSpec;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.clients.admin.DescribeTransactionsResult;
import org.apache.kafka.clients.admin.ListTransactionsResult;
import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTransactionRepairService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaTransactionRepairService.class);

  private final KaldbConfigs.ManagerConfig.KafkaTransactionRepairServiceConfig config;
  private AdminClient adminClient;

  public KafkaTransactionRepairService(
      KaldbConfigs.ManagerConfig.KafkaTransactionRepairServiceConfig config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    adminClient =
        AdminClient.create(
            Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.getKafkaConfig().getKafkaBootStrapServers(),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                "60000"));
  }

  @Override
  protected void shutDown() throws Exception {
    adminClient.close();
  }

  @Override
  protected void runOneIteration() throws Exception {
    int hangingTransactionCount = 0;
    int totalTransactionCount = 0;
    try {
      LOG.info("Attempting to find hanging transactions");
      // get all transactions
      ListTransactionsResult listTransactionsResult = adminClient.listTransactions();
      List<String> transactionIds =
          listTransactionsResult.all().get().stream()
              .map(TransactionListing::transactionalId)
              .toList();

      // describe all transactions
      DescribeTransactionsResult describeTransactionsResult =
          adminClient.describeTransactions(transactionIds);
      Map<String, TransactionDescription> transactionMap = describeTransactionsResult.all().get();

      // for each transaction that exists
      for (TransactionDescription transactionDescription : transactionMap.values()) {
        Set<TopicPartition> partitions = transactionDescription.topicPartitions();
        DescribeProducersResult describeProducersResult = adminClient.describeProducers(partitions);

        // foreach topic partition
        for (TopicPartition topicPartition : partitions) {
          if (topicPartition.topic().equals(config.getKafkaConfig().getKafkaTopic())) {
            totalTransactionCount++;
            DescribeProducersResult.PartitionProducerState partitionProducerState =
                describeProducersResult.partitionResult(topicPartition).get();

            // if the transaction has reached a max timeout, automatically abort it
            if (transactionDescription.transactionStartTimeMs().isPresent()) {
              // todo - make cutoff configurable
              if (transactionDescription.transactionStartTimeMs().getAsLong()
                  < Instant.now().minusSeconds(300).toEpochMilli()) {
                hangingTransactionCount++;
                LOG.info("Attempting to abort transaction - {}", transactionDescription);

                for (ProducerState activeProducer : partitionProducerState.activeProducers()) {
                  AbortTransactionSpec abortTransactionSpec =
                      new AbortTransactionSpec(
                          topicPartition,
                          transactionDescription.producerId(),
                          (short) activeProducer.producerEpoch(),
                          activeProducer.coordinatorEpoch().orElse(0));

                  LOG.info("Aborting transaction using spec - {}", abortTransactionSpec);
                  // adminClient.abortTransaction(abortTransactionSpec).all();
                }
              } else {
                LOG.info("Found transaction that appeared valid {}", transactionDescription);
              }
            } else {
              LOG.warn("Transaction description missing start time, {}", transactionDescription);
            }
          }
        }
      }
      LOG.info("Examined transaction count {}, found hanging transaction count {}", totalTransactionCount, hangingTransactionCount);
    } catch (Exception e) {
      LOG.error("Error running kafka transaction repair service", e);
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(1, config.getSchedulePeriodMins(), TimeUnit.MINUTES);
  }
}
