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
import com.slack.kaldb.writer.KafkaUtils;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkIngestKafkaProducer extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(BulkIngestKafkaProducer.class);

  private KafkaProducer<String, byte[]> kafkaProducer;

  private KafkaClientMetrics kafkaMetrics;

  private final KaldbConfigs.KafkaConfig kafkaConfig;

  private final DatasetMetadataStore datasetMetadataStore;
  private final KaldbMetadataStoreChangeListener<DatasetMetadata> datasetListener =
      (_) -> cacheSortedDataset();

  protected List<DatasetMetadata> throughputSortedDatasets;

  private final BlockingQueue<BulkIngestRequest> pendingRequests;

  private final Integer producerSleepMs;

  public static final String FAILED_SET_RESPONSE_COUNTER =
      "bulk_ingest_producer_failed_set_response";
  private final Counter failedSetResponseCounter;
  public static final String STALL_COUNTER = "bulk_ingest_producer_stall_counter";
  private final Counter stallCounter;

  public static final String KAFKA_RESTART_COUNTER = "bulk_ingest_producer_kafka_restart_timer";

  private final Timer kafkaRestartTimer;

  public static final String BATCH_SIZE_GAUGE = "bulk_ingest_producer_batch_size";
  private final AtomicInteger batchSizeGauge;

  private final MeterRegistry meterRegistry;

  private static final Set<String> OVERRIDABLE_CONFIGS =
      Set.of(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

  public BulkIngestKafkaProducer(
      final DatasetMetadataStore datasetMetadataStore,
      final KaldbConfigs.PreprocessorConfig preprocessorConfig,
      final PrometheusMeterRegistry meterRegistry) {
    this.kafkaConfig = preprocessorConfig.getKafkaConfig();

    checkArgument(
        !kafkaConfig.getKafkaBootStrapServers().isEmpty(),
        "Kafka bootstrapServers must be provided");
    checkArgument(!kafkaConfig.getKafkaTopic().isEmpty(), "Kafka topic must be provided");

    this.meterRegistry = meterRegistry;
    this.datasetMetadataStore = datasetMetadataStore;
    this.pendingRequests = new LinkedBlockingQueue<>();

    // todo - consider making this a configurable value or removing the config
    this.producerSleepMs =
        Integer.parseInt(System.getProperty("kalDb.bulkIngest.producerSleepMs", "50"));

    this.failedSetResponseCounter = meterRegistry.counter(FAILED_SET_RESPONSE_COUNTER);
    this.stallCounter = meterRegistry.counter(STALL_COUNTER);
    this.kafkaRestartTimer = meterRegistry.timer(KAFKA_RESTART_COUNTER);
    this.batchSizeGauge = meterRegistry.gauge(BATCH_SIZE_GAUGE, new AtomicInteger(0));

    startKafkaProducer();
  }

  private void startKafkaProducer() {
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

  private void stopKafkaProducer() {
    try {
      if (this.kafkaProducer != null) {
        this.kafkaProducer.close(Duration.ZERO);
      }

      if (this.kafkaMetrics != null) {
        this.kafkaMetrics.close();
      }
    } catch (Exception e) {
      LOG.error("Error attempting to stop the Kafka producer", e);
    }
  }

  private void restartKafkaProducer() {
    Timer.Sample restartTimer = Timer.start(meterRegistry);
    stopKafkaProducer();
    startKafkaProducer();
    LOG.info("Restarted the kafka producer");
    restartTimer.stop(kafkaRestartTimer);
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
      List<BulkIngestRequest> requests = new ArrayList<>();
      pendingRequests.drainTo(requests);
      batchSizeGauge.set(requests.size());
      if (requests.isEmpty()) {
        try {
          stallCounter.increment();
          Thread.sleep(producerSleepMs);
        } catch (InterruptedException e) {
          return;
        }
      } else {
        produceDocumentsAndCommit(requests);
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

  public BulkIngestRequest submitRequest(Map<String, List<Trace.Span>> inputDocs) {
    BulkIngestRequest request = new BulkIngestRequest(inputDocs);
    pendingRequests.add(request);
    return request;
  }

  protected Map<BulkIngestRequest, BulkIngestResponse> produceDocumentsAndCommit(
      List<BulkIngestRequest> requests) {
    Map<BulkIngestRequest, BulkIngestResponse> responseMap = new HashMap<>();
    try {
      kafkaProducer.beginTransaction();
      for (BulkIngestRequest request : requests) {
        responseMap.put(request, produceDocuments(request.getInputDocs(), kafkaProducer));
      }
      kafkaProducer.commitTransaction();
    } catch (TimeoutException te) {
      // todo - consider collapsing these exceptions into a common implementation

      // In the event of a timeout, we cannot abort but must either retry or restart the producer
      // See org.apache.kafka.clients.producer.KafkaProducer.abortTransaction docblock
      LOG.error("Commit transaction timeout, must restart producer", te);
      restartKafkaProducer();

      for (BulkIngestRequest request : requests) {
        responseMap.put(
            request,
            new BulkIngestResponse(
                0,
                request.getInputDocs().values().stream().mapToInt(List::size).sum(),
                te.getMessage()));
      }
    } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
      // We can't recover from these exceptions, so our only option is to close the producer and
      // exit.
      LOG.error("Unrecoverable kafka error, must restart producer", e);
      restartKafkaProducer();

      for (BulkIngestRequest request : requests) {
        responseMap.put(
            request,
            new BulkIngestResponse(
                0,
                request.getInputDocs().values().stream().mapToInt(List::size).sum(),
                e.getMessage()));
      }
    } catch (Exception e) {
      LOG.warn("failed transaction with error", e);
      if (kafkaProducer != null) {
        try {
          kafkaProducer.abortTransaction();
        } catch (ProducerFencedException err) {
          LOG.error("Could not abort transaction, must restart producer", err);
          restartKafkaProducer();
        }
      }

      for (BulkIngestRequest request : requests) {
        responseMap.put(
            request,
            new BulkIngestResponse(
                0,
                request.getInputDocs().values().stream().mapToInt(List::size).sum(),
                e.getMessage()));
      }
    }

    for (Map.Entry<BulkIngestRequest, BulkIngestResponse> entry : responseMap.entrySet()) {
      BulkIngestRequest key = entry.getKey();
      BulkIngestResponse value = entry.getValue();
      if (!key.setResponse(value)) {
        LOG.warn("Failed to add result to the bulk ingest request, consumer thread went away?");
        failedSetResponseCounter.increment();
      }
    }
    return responseMap;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private BulkIngestResponse produceDocuments(
      Map<String, List<Trace.Span>> indexDocs, KafkaProducer<String, byte[]> kafkaProducer) {
    int totalDocs = indexDocs.values().stream().mapToInt(List::size).sum();

    // we cannot create a generic pool of producers because the kafka API expects the transaction ID
    // to be a property while creating the producer object.
    for (Map.Entry<String, List<Trace.Span>> indexDoc : indexDocs.entrySet()) {
      String index = indexDoc.getKey();

      // call once per batch and use the same partition for better batching
      // todo - this probably shouldn't be tied to the transaction batching logic?
      int partition = getPartition(index);
      meterRegistry.counter("bulk_ingest_producer_kafka_partition", Tags.of("partitionId", String.valueOf(partition))).increment();

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
      for (Trace.Span doc : indexDoc.getValue()) {
        ProducerRecord<String, byte[]> producerRecord =
            new ProducerRecord<>(kafkaConfig.getKafkaTopic(), partition, index, doc.toByteArray());

        // we intentionally suppress FutureReturnValueIgnored here in errorprone - this is because
        // we wrap this in a transaction, which is responsible for flushing all of the pending
        // messages
        kafkaProducer.send(producerRecord);
      }
    }

    return new BulkIngestResponse(totalDocs, 0, "");
  }

  private KafkaProducer<String, byte[]> createKafkaTransactionProducer(String transactionId) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getKafkaBootStrapServers());
    props.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);

    // don't override the properties that we have already set explicitly using named properties
    for (Map.Entry<String, String> additionalProp :
        kafkaConfig.getAdditionalPropsMap().entrySet()) {
      props =
          KafkaUtils.maybeOverrideProps(
              props,
              additionalProp.getKey(),
              additionalProp.getValue(),
              OVERRIDABLE_CONFIGS.contains(additionalProp.getKey()));
    }
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
