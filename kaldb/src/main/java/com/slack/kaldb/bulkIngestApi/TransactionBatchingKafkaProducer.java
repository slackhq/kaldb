package com.slack.kaldb.bulkIngestApi;

import static com.google.common.base.Preconditions.checkArgument;
import static com.slack.kaldb.metadata.dataset.DatasetMetadata.MATCH_ALL_SERVICE;
import static com.slack.kaldb.metadata.dataset.DatasetMetadata.MATCH_STAR_SERVICE;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.slack.kaldb.metadata.core.KaldbMetadataStoreChangeListener;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.preprocessor.PreprocessorService;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.util.RuntimeHalterImpl;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionBatchingKafkaProducer extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionBatchingKafkaProducer.class);

  private final KafkaProducer kafkaProducer;
  private final KafkaClientMetrics kafkaMetrics;

  private final KaldbConfigs.PreprocessorConfig preprocessorConfig;

  private final DatasetMetadataStore datasetMetadataStore;
  private final KaldbMetadataStoreChangeListener<DatasetMetadata> datasetListener =
      (datasetMetadata) -> cacheSortedDataset();

  protected List<DatasetMetadata> throughputSortedDatasets;

  private final BlockingQueue<BatchRequest> pendingRequests = new ArrayBlockingQueue<>(500);

  public TransactionBatchingKafkaProducer(
      final DatasetMetadataStore datasetMetadataStore,
      final KaldbConfigs.PreprocessorConfig preprocessorConfig,
      final PrometheusMeterRegistry meterRegistry) {
    checkArgument(
        !preprocessorConfig.getBootstrapServers().isEmpty(),
        "Kafka bootstrapServers must be provided");

    checkArgument(
        !preprocessorConfig.getDownstreamTopic().isEmpty(),
        "Kafka downstreamTopic must be provided");

    this.preprocessorConfig = preprocessorConfig;
    this.datasetMetadataStore = datasetMetadataStore;

    // since we use a new transaction ID every time we start a preprocessor there can be some zombie
    // transactions?
    // I think they will remain in kafka till they expire. They should never be readable if the
    // consumer sets isolation.level as "read_committed"
    // see "zombie fencing" https://www.confluent.io/blog/transactions-apache-kafka/
    this.kafkaProducer = createKafkaTransactionProducer(UUID.randomUUID().toString());

    this.kafkaMetrics = new KafkaClientMetrics(kafkaProducer);
    this.kafkaMetrics.bindTo(meterRegistry);

    this.kafkaProducer.initTransactions();
  }

  private void cacheSortedDataset() {
    // we sort the datasets to rank from which dataset do we start matching candidate service names
    // in the future we can change the ordering from sort to something else
    this.throughputSortedDatasets =
        datasetMetadataStore.listSync().stream()
            .sorted(Comparator.comparingLong(DatasetMetadata::getThroughputBytes).reversed())
            .toList();
  }

  @Override
  protected void startUp() throws Exception {
    cacheSortedDataset();
    datasetMetadataStore.addListener(datasetListener);
  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      List<BatchRequest> requests = new ArrayList<>();
      pendingRequests.drainTo(requests);
      if (requests.isEmpty()) {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          return;
        }
      } else {
        transactionCommit(requests);
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    datasetMetadataStore.removeListener(datasetListener);

    kafkaProducer.close();
    if (kafkaMetrics != null) {
      kafkaMetrics.close();
    }
  }

  public BatchRequest createRequest(Map<String, List<Trace.Span>> inputDocs) {
    BatchRequest request = new BatchRequest(inputDocs);
    // todo - add can throw exceptions
    pendingRequests.add(request);
    return request;
  }

  protected Map<BatchRequest, BulkIngestResponse> transactionCommit(List<BatchRequest> requests) {
    Map<BatchRequest, BulkIngestResponse> responseMap = new HashMap<>();
    try {
      kafkaProducer.beginTransaction();
      for (BatchRequest request : requests) {
        responseMap.put(request, produceDocuments(request.getInputDocs()));
      }
      kafkaProducer.commitTransaction();
    } catch (Exception e) {
      LOG.warn("failed transaction with error", e);
      try {
        kafkaProducer.abortTransaction();
      } catch (ProducerFencedException err) {
        LOG.error("Could not abort transaction", err);
      }

      for (BatchRequest request : requests) {
        responseMap.put(
            request,
            new BulkIngestResponse(
                0,
                request.getInputDocs().values().stream().mapToInt(List::size).sum(),
                e.getMessage()));
      }
    }

    for (Map.Entry<BatchRequest, BulkIngestResponse> entry : responseMap.entrySet()) {
      BatchRequest key = entry.getKey();
      BulkIngestResponse value = entry.getValue();
      key.setResponse(value);
    }
    return responseMap;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private BulkIngestResponse produceDocuments(Map<String, List<Trace.Span>> indexDocs) {
    int totalDocs = indexDocs.values().stream().mapToInt(List::size).sum();

    // we cannot create a generic pool of producers because the kafka API expects the transaction ID
    // to be a property while creating the producer object.
    for (Map.Entry<String, List<Trace.Span>> indexDoc : indexDocs.entrySet()) {
      String index = indexDoc.getKey();

      // call once per batch and use the same partition for better batching
      // todo - this probably shouldn't be tied to the transaction batching logic?
      int partition = getPartition(index);

      // since there isn't a dataset provisioned for this service/index we will not index this set
      // of docs
      if (partition < 0) {
        LOG.warn("index=" + index + " does not have a provisioned dataset associated with it");
        continue;
      }

      // KafkaProducer does not allow creating multiple transactions from a single object -
      // rightfully so.
      // Till we fix the producer design to allow for multiple /_bulk requests to be able to
      // write to the same txn
      // we will limit producing documents 1 thread at a time
      try {
        for (Trace.Span doc : indexDoc.getValue()) {
          ProducerRecord<String, byte[]> producerRecord =
              new ProducerRecord<>(
                  preprocessorConfig.getDownstreamTopic(), partition, index, doc.toByteArray());

          // we intentionally supress FutureReturnValueIgnored here in errorprone - this is because
          // we wrap this in a transaction, which is responsible for flushing all of the pending
          // messages
          kafkaProducer.send(producerRecord);
        }
      } catch (TimeoutException te) {
        LOG.error("Commit transaction timeout", te);
        // the commitTransaction waits till "max.block.ms" after which it will time out
        // in that case we cannot call abort exception because that throws the following error
        // "Cannot attempt operation `abortTransaction` because the previous
        // call to `commitTransaction` timed out and must be retried"
        // so for now we just restart the preprocessor
        new RuntimeHalterImpl()
            .handleFatal(
                new Throwable(
                    "KafkaProducer needs to shutdown as we don't have retry yet and we cannot call abortTxn on timeout",
                    te));
      } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
        // We can't recover from these exceptions, so our only option is to close the producer and
        // exit.
        new RuntimeHalterImpl().handleFatal(new Throwable("KafkaProducer needs to shutdown ", e));
      }
    }

    return new BulkIngestResponse(totalDocs, 0, "");
  }

  private KafkaProducer<String, byte[]> createKafkaTransactionProducer(String transactionId) {
    Properties props = new Properties();
    props.put("bootstrap.servers", preprocessorConfig.getBootstrapServers());
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("transactional.id", transactionId);
    props.put("linger.ms", 250);
    props.put("max.block.ms", "10000");
    props.put("compression.type", "snappy");
    return new KafkaProducer<>(props);
  }

  private int getPartition(String index) {
    for (DatasetMetadata datasetMetadata : throughputSortedDatasets) {
      String serviceNamePattern = datasetMetadata.getServiceNamePattern();

      if (serviceNamePattern.equals(MATCH_ALL_SERVICE)
          || serviceNamePattern.equals(MATCH_STAR_SERVICE)
          || index.equals(serviceNamePattern)) {
        List<Integer> partitions = PreprocessorService.getActivePartitionList(datasetMetadata);
        return partitions.get(ThreadLocalRandom.current().nextInt(partitions.size()));
      }
    }
    // We don't have a provisioned service for this index
    return -1;
  }
}
