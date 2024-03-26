package com.slack.kaldb.server;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.blobfs.s3.S3CrtBlobFs;
import com.slack.kaldb.bulkIngestApi.BulkIngestApi;
import com.slack.kaldb.bulkIngestApi.BulkIngestKafkaProducer;
import com.slack.kaldb.bulkIngestApi.DatasetRateLimitingService;
import com.slack.kaldb.chunkManager.CachingChunkManager;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.clusterManager.ClusterHpaMetricService;
import com.slack.kaldb.clusterManager.ClusterMonitorService;
import com.slack.kaldb.clusterManager.RecoveryTaskAssignmentService;
import com.slack.kaldb.clusterManager.ReplicaAssignmentService;
import com.slack.kaldb.clusterManager.ReplicaCreationService;
import com.slack.kaldb.clusterManager.ReplicaDeletionService;
import com.slack.kaldb.clusterManager.ReplicaEvictionService;
import com.slack.kaldb.clusterManager.ReplicaRestoreService;
import com.slack.kaldb.clusterManager.SnapshotDeletionService;
import com.slack.kaldb.elasticsearchApi.ElasticsearchApiService;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.KaldbDistributedQueryService;
import com.slack.kaldb.logstore.search.KaldbLocalQueryService;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.core.CloseableLifecycleManager;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.metadata.hpa.HpaMetricMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.schema.SchemaUtil;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.preprocessor.PreprocessorService;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.proto.schema.Schema;
import com.slack.kaldb.recovery.RecoveryService;
import com.slack.kaldb.util.RuntimeHalterImpl;
import com.slack.kaldb.zipkinApi.ZipkinService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * Main class of Kaldb that sets up the basic infra needed for all the other end points like an a
 * http server, register monitoring libraries, create config manager etc..
 */
public class Kaldb {
  private static final Logger LOG = LoggerFactory.getLogger(Kaldb.class);

  private final PrometheusMeterRegistry prometheusMeterRegistry;

  private final KaldbConfigs.KaldbConfig kaldbConfig;
  private final S3AsyncClient s3Client;
  protected ServiceManager serviceManager;
  protected AsyncCuratorFramework curatorFramework;

  Kaldb(
      KaldbConfigs.KaldbConfig kaldbConfig,
      S3AsyncClient s3Client,
      PrometheusMeterRegistry prometheusMeterRegistry) {
    this.prometheusMeterRegistry = prometheusMeterRegistry;
    this.kaldbConfig = kaldbConfig;
    this.s3Client = s3Client;
    Metrics.addRegistry(prometheusMeterRegistry);
    LOG.info("Started Kaldb process with config: {}", kaldbConfig);
  }

  Kaldb(KaldbConfigs.KaldbConfig kaldbConfig, PrometheusMeterRegistry prometheusMeterRegistry) {
    this(kaldbConfig, S3CrtBlobFs.initS3Client(kaldbConfig.getS3Config()), prometheusMeterRegistry);
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      LOG.info("Config file is needed a first argument");
    }
    Path configFilePath = Path.of(args[0]);

    KaldbConfig.initFromFile(configFilePath);
    KaldbConfigs.KaldbConfig config = KaldbConfig.get();
    Kaldb kalDb = new Kaldb(KaldbConfig.get(), initPrometheusMeterRegistry(config));
    kalDb.start();
  }

  static PrometheusMeterRegistry initPrometheusMeterRegistry(KaldbConfigs.KaldbConfig config) {
    PrometheusMeterRegistry prometheusMeterRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    prometheusMeterRegistry
        .config()
        .commonTags(
            "kaldb_cluster_name",
            config.getClusterConfig().getClusterName(),
            "kaldb_env",
            config.getClusterConfig().getEnv(),
            "kaldb_component",
            getComponentTag(config));
    return prometheusMeterRegistry;
  }

  private static String getComponentTag(KaldbConfigs.KaldbConfig config) {
    String component;
    if (config.getNodeRolesList().size() == 1) {
      component = config.getNodeRolesList().get(0).toString();
    } else {
      component = Strings.join(config.getNodeRolesList(), '-');
    }
    return Strings.toRootLowerCase(component);
  }

  public void start() throws Exception {
    setupSystemMetrics(prometheusMeterRegistry);
    addShutdownHook();

    curatorFramework =
        CuratorBuilder.build(
            prometheusMeterRegistry, kaldbConfig.getMetadataStoreConfig().getZookeeperConfig());

    // Initialize blobfs. Only S3 is supported currently.
    S3CrtBlobFs s3BlobFs = new S3CrtBlobFs(s3Client);

    Set<Service> services =
        getServices(curatorFramework, kaldbConfig, s3BlobFs, prometheusMeterRegistry);
    serviceManager = new ServiceManager(services);
    serviceManager.addListener(getServiceManagerListener(), MoreExecutors.directExecutor());

    serviceManager.startAsync();
  }

  private static Set<Service> getServices(
      AsyncCuratorFramework curatorFramework,
      KaldbConfigs.KaldbConfig kaldbConfig,
      BlobFs blobFs,
      PrometheusMeterRegistry meterRegistry)
      throws Exception {
    Set<Service> services = new HashSet<>();

    HashSet<KaldbConfigs.NodeRole> roles = new HashSet<>(kaldbConfig.getNodeRolesList());

    if (roles.contains(KaldbConfigs.NodeRole.INDEX)) {
      IndexingChunkManager<LogMessage> chunkManager =
          IndexingChunkManager.fromConfig(
              meterRegistry,
              curatorFramework,
              kaldbConfig.getIndexerConfig(),
              blobFs,
              kaldbConfig.getS3Config());
      services.add(chunkManager);

      KaldbIndexer indexer =
          new KaldbIndexer(
              chunkManager,
              curatorFramework,
              kaldbConfig.getIndexerConfig(),
              kaldbConfig.getIndexerConfig().getKafkaConfig(),
              meterRegistry);
      services.add(indexer);

      KaldbLocalQueryService<LogMessage> searcher =
          new KaldbLocalQueryService<>(
              chunkManager,
              Duration.ofMillis(kaldbConfig.getIndexerConfig().getDefaultQueryTimeoutMs()));
      final int serverPort = kaldbConfig.getIndexerConfig().getServerConfig().getServerPort();
      Duration requestTimeout =
          Duration.ofMillis(kaldbConfig.getIndexerConfig().getServerConfig().getRequestTimeoutMs());
      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "kalDbIndex", meterRegistry)
              .withRequestTimeout(requestTimeout)
              .withTracing(kaldbConfig.getTracingConfig())
              .withGrpcService(searcher)
              .build();
      services.add(armeriaService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.QUERY)) {
      SearchMetadataStore searchMetadataStore =
          new SearchMetadataStore(curatorFramework, true, meterRegistry);
      SnapshotMetadataStore snapshotMetadataStore =
          new SnapshotMetadataStore(curatorFramework, meterRegistry);
      DatasetMetadataStore datasetMetadataStore =
          new DatasetMetadataStore(curatorFramework, true, meterRegistry);

      services.add(
          new CloseableLifecycleManager(
              KaldbConfigs.NodeRole.QUERY,
              List.of(searchMetadataStore, snapshotMetadataStore, datasetMetadataStore)));

      Duration requestTimeout =
          Duration.ofMillis(kaldbConfig.getQueryConfig().getServerConfig().getRequestTimeoutMs());
      KaldbDistributedQueryService kaldbDistributedQueryService =
          new KaldbDistributedQueryService(
              searchMetadataStore,
              snapshotMetadataStore,
              datasetMetadataStore,
              meterRegistry,
              requestTimeout,
              Duration.ofMillis(kaldbConfig.getQueryConfig().getDefaultQueryTimeoutMs()));
      // todo - close the kaldbDistributedQueryService once done (depends on
      // https://github.com/slackhq/kaldb/pull/564)
      final int serverPort = kaldbConfig.getQueryConfig().getServerConfig().getServerPort();

      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "kalDbQuery", meterRegistry)
              .withRequestTimeout(requestTimeout)
              .withTracing(kaldbConfig.getTracingConfig())
              .withAnnotatedService(new ElasticsearchApiService(kaldbDistributedQueryService))
              .withAnnotatedService(new ZipkinService(kaldbDistributedQueryService))
              .withGrpcService(kaldbDistributedQueryService)
              .build();
      services.add(armeriaService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.CACHE)) {
      CachingChunkManager<LogMessage> chunkManager =
          CachingChunkManager.fromConfig(
              meterRegistry,
              curatorFramework,
              kaldbConfig.getS3Config(),
              kaldbConfig.getCacheConfig(),
              blobFs);
      services.add(chunkManager);

      HpaMetricMetadataStore hpaMetricMetadataStore =
          new HpaMetricMetadataStore(curatorFramework, true, meterRegistry);
      services.add(
          new CloseableLifecycleManager(
              KaldbConfigs.NodeRole.CACHE, List.of(hpaMetricMetadataStore)));
      HpaMetricPublisherService hpaMetricPublisherService =
          new HpaMetricPublisherService(
              hpaMetricMetadataStore, meterRegistry, Metadata.HpaMetricMetadata.NodeRole.CACHE);
      services.add(hpaMetricPublisherService);

      KaldbLocalQueryService<LogMessage> searcher =
          new KaldbLocalQueryService<>(
              chunkManager,
              Duration.ofMillis(kaldbConfig.getCacheConfig().getDefaultQueryTimeoutMs()));
      final int serverPort = kaldbConfig.getCacheConfig().getServerConfig().getServerPort();
      Duration requestTimeout =
          Duration.ofMillis(kaldbConfig.getCacheConfig().getServerConfig().getRequestTimeoutMs());
      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "kalDbCache", meterRegistry)
              .withRequestTimeout(requestTimeout)
              .withTracing(kaldbConfig.getTracingConfig())
              .withGrpcService(searcher)
              .build();
      services.add(armeriaService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.MANAGER)) {
      final KaldbConfigs.ManagerConfig managerConfig = kaldbConfig.getManagerConfig();
      final int serverPort = managerConfig.getServerConfig().getServerPort();

      ReplicaMetadataStore replicaMetadataStore =
          new ReplicaMetadataStore(curatorFramework, meterRegistry);
      SnapshotMetadataStore snapshotMetadataStore =
          new SnapshotMetadataStore(curatorFramework, meterRegistry);
      RecoveryTaskMetadataStore recoveryTaskMetadataStore =
          new RecoveryTaskMetadataStore(curatorFramework, true, meterRegistry);
      RecoveryNodeMetadataStore recoveryNodeMetadataStore =
          new RecoveryNodeMetadataStore(curatorFramework, true, meterRegistry);
      CacheSlotMetadataStore cacheSlotMetadataStore =
          new CacheSlotMetadataStore(curatorFramework, meterRegistry);
      DatasetMetadataStore datasetMetadataStore =
          new DatasetMetadataStore(curatorFramework, true, meterRegistry);
      HpaMetricMetadataStore hpaMetricMetadataStore =
          new HpaMetricMetadataStore(curatorFramework, true, meterRegistry);

      Duration requestTimeout =
          Duration.ofMillis(kaldbConfig.getManagerConfig().getServerConfig().getRequestTimeoutMs());
      ReplicaRestoreService replicaRestoreService =
          new ReplicaRestoreService(replicaMetadataStore, meterRegistry, managerConfig);
      services.add(replicaRestoreService);

      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "kalDbManager", meterRegistry)
              .withRequestTimeout(requestTimeout)
              .withTracing(kaldbConfig.getTracingConfig())
              .withGrpcService(
                  new ManagerApiGrpc(
                      datasetMetadataStore, snapshotMetadataStore, replicaRestoreService))
              .build();
      services.add(armeriaService);

      services.add(
          new CloseableLifecycleManager(
              KaldbConfigs.NodeRole.MANAGER,
              List.of(
                  replicaMetadataStore,
                  snapshotMetadataStore,
                  recoveryTaskMetadataStore,
                  recoveryNodeMetadataStore,
                  cacheSlotMetadataStore,
                  datasetMetadataStore,
                  hpaMetricMetadataStore)));

      ReplicaCreationService replicaCreationService =
          new ReplicaCreationService(
              replicaMetadataStore, snapshotMetadataStore, managerConfig, meterRegistry);
      services.add(replicaCreationService);

      ReplicaEvictionService replicaEvictionService =
          new ReplicaEvictionService(
              cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);
      services.add(replicaEvictionService);

      ReplicaDeletionService replicaDeletionService =
          new ReplicaDeletionService(
              cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);
      services.add(replicaDeletionService);

      RecoveryTaskAssignmentService recoveryTaskAssignmentService =
          new RecoveryTaskAssignmentService(
              recoveryTaskMetadataStore, recoveryNodeMetadataStore, managerConfig, meterRegistry);
      services.add(recoveryTaskAssignmentService);

      ReplicaAssignmentService replicaAssignmentService =
          new ReplicaAssignmentService(
              cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);
      services.add(replicaAssignmentService);

      SnapshotDeletionService snapshotDeletionService =
          new SnapshotDeletionService(
              replicaMetadataStore, snapshotMetadataStore, blobFs, managerConfig, meterRegistry);
      services.add(snapshotDeletionService);

      ClusterMonitorService clusterMonitorService =
          new ClusterMonitorService(
              replicaMetadataStore,
              snapshotMetadataStore,
              recoveryTaskMetadataStore,
              recoveryNodeMetadataStore,
              cacheSlotMetadataStore,
              datasetMetadataStore,
              meterRegistry);
      services.add(clusterMonitorService);

      ClusterHpaMetricService clusterHpaMetricService =
          new ClusterHpaMetricService(
              replicaMetadataStore, cacheSlotMetadataStore, hpaMetricMetadataStore);
      services.add(clusterHpaMetricService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.RECOVERY)) {
      final KaldbConfigs.RecoveryConfig recoveryConfig = kaldbConfig.getRecoveryConfig();
      final int serverPort = recoveryConfig.getServerConfig().getServerPort();

      Duration requestTimeout =
          Duration.ofMillis(
              kaldbConfig.getRecoveryConfig().getServerConfig().getRequestTimeoutMs());
      ArmeriaService armeriaService =
          new ArmeriaService.Builder(serverPort, "kalDbRecovery", meterRegistry)
              .withRequestTimeout(requestTimeout)
              .withTracing(kaldbConfig.getTracingConfig())
              .build();
      services.add(armeriaService);

      RecoveryService recoveryService =
          new RecoveryService(kaldbConfig, curatorFramework, meterRegistry, blobFs);
      services.add(recoveryService);
    }

    if (roles.contains(KaldbConfigs.NodeRole.PREPROCESSOR)) {
      DatasetMetadataStore datasetMetadataStore =
          new DatasetMetadataStore(curatorFramework, true, meterRegistry);

      final KaldbConfigs.PreprocessorConfig preprocessorConfig =
          kaldbConfig.getPreprocessorConfig();
      final int serverPort = preprocessorConfig.getServerConfig().getServerPort();

      Duration requestTimeout =
          Duration.ofMillis(
              kaldbConfig.getPreprocessorConfig().getServerConfig().getRequestTimeoutMs());
      ArmeriaService.Builder armeriaServiceBuilder =
          new ArmeriaService.Builder(serverPort, "kalDbPreprocessor", meterRegistry)
              .withRequestTimeout(requestTimeout)
              .withTracing(kaldbConfig.getTracingConfig());

      services.add(
          new CloseableLifecycleManager(
              KaldbConfigs.NodeRole.PREPROCESSOR, List.of(datasetMetadataStore)));

      if (preprocessorConfig.getUseBulkApi()) {
        BulkIngestKafkaProducer bulkIngestKafkaProducer =
            new BulkIngestKafkaProducer(datasetMetadataStore, preprocessorConfig, meterRegistry);
        services.add(bulkIngestKafkaProducer);
        DatasetRateLimitingService datasetRateLimitingService =
            new DatasetRateLimitingService(datasetMetadataStore, preprocessorConfig, meterRegistry);
        services.add(datasetRateLimitingService);

        Schema.IngestSchema schema = Schema.IngestSchema.getDefaultInstance();
        if (!preprocessorConfig.getSchemaFile().isEmpty()) {
          LOG.info("Loading schema file: {}", preprocessorConfig.getSchemaFile());
          schema = SchemaUtil.parseSchema(Path.of(preprocessorConfig.getSchemaFile()));
          LOG.info(
              "Loaded schema from file: {} with total fields: {}", schema, schema.getFieldsCount());
        } else {
          LOG.info("No schema file provided, using default schema");
        }
        BulkIngestApi openSearchBulkApiService =
            new BulkIngestApi(
                bulkIngestKafkaProducer,
                datasetRateLimitingService,
                meterRegistry,
                preprocessorConfig.getRateLimitExceededErrorCode(),
                schema);
        armeriaServiceBuilder.withAnnotatedService(openSearchBulkApiService);
      } else {
        PreprocessorService preprocessorService =
            new PreprocessorService(datasetMetadataStore, preprocessorConfig, meterRegistry);
        services.add(preprocessorService);
      }
      services.add(armeriaServiceBuilder.build());
    }

    return services;
  }

  private static ServiceManager.Listener getServiceManagerListener() {
    return new ServiceManager.Listener() {
      @Override
      public void failure(Service service) {
        LOG.error(
            String.format("Service %s failed with cause ", service.getClass().toString()),
            service.failureCause());
        // shutdown if any services enters failure state
        new RuntimeHalterImpl()
            .handleFatal(new Throwable("Shutting down Kaldb due to failed service"));
      }
    };
  }

  void shutdown() {
    LOG.info("Running shutdown hook.");
    try {
      serviceManager.stopAsync().awaitStopped(30, TimeUnit.SECONDS);
    } catch (Exception e) {
      // stopping timed out
      LOG.error("ServiceManager shutdown timed out", e);
    }
    try {
      curatorFramework.unwrap().close();
    } catch (Exception e) {
      LOG.error("Error while closing curatorFramework ", e);
    }
    LOG.info("Shutting down LogManager");
    LogManager.shutdown();
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
  }

  private static void setupSystemMetrics(MeterRegistry prometheusMeterRegistry) {
    // Expose JVM metrics.
    new ClassLoaderMetrics().bindTo(prometheusMeterRegistry);
    new JvmMemoryMetrics().bindTo(prometheusMeterRegistry);
    new JvmGcMetrics().bindTo(prometheusMeterRegistry);
    new ProcessorMetrics().bindTo(prometheusMeterRegistry);
    new JvmThreadMetrics().bindTo(prometheusMeterRegistry);

    LOG.info("Done registering standard JVM metrics for indexer service");
  }
}
