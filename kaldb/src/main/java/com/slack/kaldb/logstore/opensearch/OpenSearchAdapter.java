package com.slack.kaldb.logstore.opensearch;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.aggregations.AggBuilder;
import com.slack.kaldb.logstore.search.aggregations.AggBuilderBase;
import com.slack.kaldb.logstore.search.aggregations.AutoDateHistogramAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.AvgAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.CumulativeSumAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.DerivativeAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.ExtendedStatsAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.FiltersAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.HistogramAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.MaxAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.MinAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.MovingAvgAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.MovingFunctionAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.PercentilesAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.SumAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.TermsAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.UniqueCountAggBuilder;
import com.slack.kaldb.metadata.schema.FieldType;
import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.IndexFieldDataService;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.indices.IndicesModule;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.BucketHelpers;
import org.opensearch.search.aggregations.pipeline.CumulativeSumPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.EwmaModel;
import org.opensearch.search.aggregations.pipeline.HoltLinearModel;
import org.opensearch.search.aggregations.pipeline.HoltWintersModel;
import org.opensearch.search.aggregations.pipeline.LinearModel;
import org.opensearch.search.aggregations.pipeline.MovAvgModel;
import org.opensearch.search.aggregations.pipeline.MovAvgPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.MovFnPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.aggregations.pipeline.SimpleModel;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.internal.SearchContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to allow using OpenSearch aggregations and query parsing from within Kaldb. This
 * class should ultimately act as an adapter where OpenSearch code is not needed external to this
 * class. <br>
 * TODO - implement a custom InternalAggregation and return these instead of the OpenSearch
 * InternalAggregation classes
 */
public class OpenSearchAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchAdapter.class);

  private final IndexSettings indexSettings;
  private final SimilarityService similarityService;

  private final MapperService mapperService;

  private final Map<String, LuceneFieldDef> chunkSchema;

  // we can make this configurable when SchemaAwareLogDocumentBuilderImpl enforces a limit
  // set this to a high number for now
  private static final int TOTAL_FIELDS_LIMIT = 2500;

  public OpenSearchAdapter(Map<String, LuceneFieldDef> chunkSchema) {
    this.indexSettings = buildIndexSettings();
    this.similarityService = new SimilarityService(indexSettings, null, emptyMap());
    this.mapperService = buildMapperService(indexSettings, similarityService);
    this.chunkSchema = chunkSchema;
  }

  /**
   * Builds a Lucene query using the provided arguments, and the currently loaded schema. Uses the
   * Opensearch Query String builder. TODO - use the dataset param in building query
   *
   * @see <a href="https://opensearch.org/docs/latest/query-dsl/full-text/query-string/">Query
   *     parsing OpenSearch docs</a>
   * @see <a
   *     href="https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html">Query
   *     parsing ES docs</a>
   */
  public Query buildQuery(
      String dataset,
      String queryStr,
      Long startTimeMsEpoch,
      Long endTimeMsEpoch,
      IndexSearcher indexSearcher)
      throws IOException {
    LOG.trace("Query raw input string: '{}'", queryStr);
    QueryShardContext queryShardContext =
        buildQueryShardContext(
            KaldbBigArrays.getInstance(),
            indexSettings,
            indexSearcher,
            similarityService,
            mapperService);
    try {
      BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

      // only add a range filter if either start or end time is provided
      if (startTimeMsEpoch != null || endTimeMsEpoch != null) {
        RangeQueryBuilder rangeQueryBuilder =
            new RangeQueryBuilder(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);

        // todo - consider supporting something other than GTE/LTE (ie GT/LT?)
        if (startTimeMsEpoch != null) {
          rangeQueryBuilder.gte(startTimeMsEpoch);
        }

        if (endTimeMsEpoch != null) {
          rangeQueryBuilder.lte(endTimeMsEpoch);
        }

        boolQueryBuilder.filter(rangeQueryBuilder);
      }

      // todo - dataset?

      // Only add the query string clause if this is not attempting to fetch all records
      // Since we do analyze the wildcard this can cause unexpected behavior if only a wildcard is
      // provided
      if (queryStr != null
          && !queryStr.isEmpty()
          && !queryStr.equals("*:*")
          && !queryStr.equals("*")) {
        QueryStringQueryBuilder queryStringQueryBuilder = new QueryStringQueryBuilder(queryStr);

        if (queryShardContext.getMapperService().fieldType(LogMessage.SystemField.ALL.fieldName)
            != null) {
          queryStringQueryBuilder.defaultField(LogMessage.SystemField.ALL.fieldName);
          // setting lenient=false will not throw error when the query fails to parse against
          // numeric fields
          queryStringQueryBuilder.lenient(false);
        } else {
          queryStringQueryBuilder.lenient(true);
        }

        queryStringQueryBuilder.analyzeWildcard(true);

        boolQueryBuilder.filter(queryStringQueryBuilder);
      }
      return boolQueryBuilder.rewrite(queryShardContext).toQuery(queryShardContext);
    } catch (Exception e) {
      LOG.error("Query parse exception", e);
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * For each defined field in the chunk schema, this will check if the field is already registered,
   * and if not attempt to register it with the mapper service
   */
  public void reloadSchema() {
    // todo - see SchemaAwareLogDocumentBuilderImpl.getDefaultLuceneFieldDefinitions
    //  this needs to be adapted to include other field types once we have support
    for (Map.Entry<String, LuceneFieldDef> entry : chunkSchema.entrySet()) {
      try {
        if (entry.getValue().fieldType == FieldType.TEXT) {
          tryRegisterField(mapperService, entry.getValue().name, b -> b.field("type", "text"));
        } else if (entry.getValue().fieldType == FieldType.STRING
            || entry.getValue().fieldType == FieldType.KEYWORD
            || entry.getValue().fieldType == FieldType.ID) {
          tryRegisterField(mapperService, entry.getValue().name, b -> b.field("type", "keyword"));
        } else if (entry.getValue().fieldType == FieldType.IP) {
          tryRegisterField(mapperService, entry.getValue().name, b -> b.field("type", "ip"));
        } else if (entry.getValue().fieldType == FieldType.DATE) {
          tryRegisterField(mapperService, entry.getValue().name, b -> b.field("type", "date"));
        } else if (entry.getValue().fieldType == FieldType.BOOLEAN) {
          tryRegisterField(mapperService, entry.getValue().name, b -> b.field("type", "boolean"));
        } else if (entry.getValue().fieldType == FieldType.DOUBLE) {
          tryRegisterField(mapperService, entry.getValue().name, b -> b.field("type", "double"));
        } else if (entry.getValue().fieldType == FieldType.FLOAT) {
          tryRegisterField(mapperService, entry.getValue().name, b -> b.field("type", "float"));
        } else if (entry.getValue().fieldType == FieldType.HALF_FLOAT) {
          tryRegisterField(
              mapperService, entry.getValue().name, b -> b.field("type", "half_float"));
        } else if (entry.getValue().fieldType == FieldType.INTEGER) {
          tryRegisterField(mapperService, entry.getValue().name, b -> b.field("type", "integer"));
        } else if (entry.getValue().fieldType == FieldType.LONG) {
          tryRegisterField(mapperService, entry.getValue().name, b -> b.field("type", "long"));
        } else if (entry.getValue().fieldType == FieldType.SCALED_LONG) {
          tryRegisterField(
              mapperService, entry.getValue().name, b -> b.field("type", "scaled_long"));
        } else if (entry.getValue().fieldType == FieldType.SHORT) {
          tryRegisterField(mapperService, entry.getValue().name, b -> b.field("type", "short"));
        } else if (entry.getValue().fieldType == FieldType.BYTE) {
          tryRegisterField(mapperService, entry.getValue().name, b -> b.field("type", "byte"));
        } else if (entry.getValue().fieldType == FieldType.BINARY) {
          tryRegisterField(mapperService, entry.getValue().name, b -> b.field("type", "binary"));
        } else {
          LOG.warn(
              "Field type '{}' is not yet currently supported for field '{}'",
              entry.getValue().fieldType,
              entry.getValue().name);
        }
      } catch (Exception e) {
        LOG.error("Error parsing schema mapping for {}", entry.getValue().toString(), e);
      }
    }
  }

  protected static XContentBuilder mapping(
      CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
    XContentBuilder builder =
        XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties");
    buildFields.accept(builder);
    return builder.endObject().endObject().endObject();
  }

  protected static XContentBuilder fieldMapping(
      String fieldName, CheckedConsumer<XContentBuilder, IOException> buildField)
      throws IOException {
    return mapping(
        b -> {
          b.startObject(fieldName);
          buildField.accept(b);
          b.endObject();
        });
  }

  /** Builds a CollectorManager for use in the Lucene aggregation step */
  public CollectorManager<Aggregator, InternalAggregation> getCollectorManager(
      AggBuilder aggBuilder, IndexSearcher indexSearcher, Query query) {
    return new CollectorManager<>() {
      @Override
      public Aggregator newCollector() throws IOException {
        Aggregator aggregator = buildAggregatorUsingContext(aggBuilder, indexSearcher, query);
        // preCollection must be invoked prior to using aggregations
        aggregator.preCollection();
        return aggregator;
      }

      /**
       * The collector manager required a collection of collectors for reducing, though for our
       * normal case this will likely only be a single collector
       */
      @Override
      public InternalAggregation reduce(Collection<Aggregator> collectors) throws IOException {
        List<InternalAggregation> internalAggregationList = new ArrayList<>();
        for (Aggregator collector : collectors) {
          // postCollection must be invoked prior to building the internal aggregations
          collector.postCollection();
          internalAggregationList.add(collector.buildTopLevel());
        }

        if (internalAggregationList.size() == 0) {
          return null;
        } else {
          // Using the first element on the list as the basis for the reduce method is per
          // OpenSearch recommendations: "For best efficiency, when implementing, try
          // reusing an existing instance (typically the first in the given list) to save
          // on redundant object construction."
          return internalAggregationList
              .get(0)
              .reduce(
                  internalAggregationList,
                  InternalAggregation.ReduceContext.forPartialReduction(
                      KaldbBigArrays.getInstance(),
                      null,
                      () -> PipelineAggregator.PipelineTree.EMPTY));
        }
      }
    };
  }

  /**
   * Registers the field types that can be aggregated by the different aggregators. Each aggregation
   * builder must be registered with the appropriate fields, or the resulting aggregation will be
   * empty.
   */
  private static ValuesSourceRegistry buildValueSourceRegistry() {
    ValuesSourceRegistry.Builder valuesSourceRegistryBuilder = new ValuesSourceRegistry.Builder();

    AutoDateHistogramAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    DateHistogramAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    HistogramAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    TermsAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    AvgAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    SumAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    MinAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    MaxAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    CardinalityAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    ExtendedStatsAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    PercentilesAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    ValueCountAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);

    // Filters are registered in a non-standard way
    valuesSourceRegistryBuilder.registerUsage(FiltersAggregationBuilder.NAME);

    return valuesSourceRegistryBuilder.build();
  }

  /** Builds the minimal amount of IndexSettings required for using Aggregations */
  protected static IndexSettings buildIndexSettings() {
    Settings settings =
        Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_2_11_0)
            .put(
                MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), TOTAL_FIELDS_LIMIT)

            // Kaldb time sorts the indexes while building it
            // {LuceneIndexStoreImpl#buildIndexWriterConfig}
            // When we were using the lucene query parser the sort info was leveraged by lucene
            // automatically ( as the sort info persists in the segment info ) at query time.
            // However the OpenSearch query parser has a custom implementation which relies on the
            // index sort info to be present as a setting here.
            .put("index.sort.field", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName)
            .put("index.sort.order", "desc")
            .build();
    return new IndexSettings(
        IndexMetadata.builder("index").settings(settings).build(), Settings.EMPTY);
  }

  /**
   * Builds a MapperService using the minimal amount of settings required for Aggregations. After
   * initializing the mapper service, individual fields will still need to be added using
   * this.registerField()
   */
  private static MapperService buildMapperService(
      IndexSettings indexSettings, SimilarityService similarityService) {
    return new MapperService(
        indexSettings,
        new IndexAnalyzers(
            singletonMap(
                "default",
                new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            emptyMap(),
            emptyMap()),
        new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
        similarityService,
        new IndicesModule(emptyList()).getMapperRegistry(),
        () -> {
          throw new UnsupportedOperationException();
        },
        () -> true,
        null);
  }

  /**
   * Minimal implementation of an OpenSearch QueryShardContext while still allowing an
   * AggregatorFactory to successfully instantiate. See AggregatorFactory.class
   */
  private static QueryShardContext buildQueryShardContext(
      BigArrays bigArrays,
      IndexSettings indexSettings,
      IndexSearcher indexSearcher,
      SimilarityService similarityService,
      MapperService mapperService) {
    final ValuesSourceRegistry valuesSourceRegistry = buildValueSourceRegistry();
    return new QueryShardContext(
        0,
        indexSettings,
        bigArrays,
        null,
        new IndexFieldDataService(
                indexSettings,
                new IndicesFieldDataCache(
                    indexSettings.getSettings(), new IndexFieldDataCache.Listener() {}),
                new NoneCircuitBreakerService(),
                mapperService)
            ::getForField,
        mapperService,
        similarityService,
        ScriptServiceProvider.getInstance(),
        null,
        null,
        null,
        indexSearcher,
        () -> Instant.now().toEpochMilli(),
        null,
        s -> false,
        () -> true,
        valuesSourceRegistry);
  }

  /**
   * Registers a field type and name to the MapperService for use in aggregations. This informs the
   * aggregators how to access a specific field and what value type it contains. registerField(
   * mapperService, LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, b -> b.field("type",
   * "long"));
   */
  private static boolean tryRegisterField(
      MapperService mapperService,
      String fieldName,
      CheckedConsumer<XContentBuilder, IOException> buildField) {
    MappedFieldType fieldType = mapperService.fieldType(fieldName);
    if (mapperService.isMetadataField(fieldName)) {
      LOG.trace("Skipping metadata field '{}'", fieldName);
      return false;
    } else if (fieldType != null) {
      LOG.trace(
          "Field '{}' already exists as typeName '{}', skipping query mapping update",
          fieldType.name(),
          fieldType.familyTypeName());
      return false;
    } else {
      try {
        XContentBuilder mapping = fieldMapping(fieldName, buildField);
        mapperService.merge(
            "_doc",
            new CompressedXContent(BytesReference.bytes(mapping)),
            MapperService.MergeReason.MAPPING_UPDATE);
      } catch (Exception e) {
        LOG.error("Error doing map update", e);
      }
      return true;
    }
  }

  /**
   * Given an aggBuilder, will use the previously initialized queryShardContext and searchContext to
   * return an OpenSearch aggregator / Lucene Collector
   */
  public Aggregator buildAggregatorUsingContext(
      AggBuilder builder, IndexSearcher indexSearcher, Query query) throws IOException {
    QueryShardContext queryShardContext =
        buildQueryShardContext(
            KaldbBigArrays.getInstance(),
            indexSettings,
            indexSearcher,
            similarityService,
            mapperService);
    SearchContext searchContext =
        new KaldbSearchContext(
            KaldbBigArrays.getInstance(), queryShardContext, indexSearcher, query);

    return getAggregationBuilder(builder)
        .build(queryShardContext, null)
        .create(searchContext, null, CardinalityUpperBound.ONE);
  }

  /**
   * Given an AggBuilder, will invoke the appropriate aggregation builder method to return the
   * abstract aggregation builder. This method is expected to be invoked from within the aggregation
   * builders to compose a nested aggregation tree.
   */
  @SuppressWarnings("rawtypes")
  public static AbstractAggregationBuilder getAggregationBuilder(AggBuilder aggBuilder) {
    if (aggBuilder.getType().equals(DateHistogramAggBuilder.TYPE)) {
      return getDateHistogramAggregationBuilder((DateHistogramAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(AutoDateHistogramAggBuilder.TYPE)) {
      return getAutoDateHistogramAggregationBuilder((AutoDateHistogramAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(HistogramAggBuilder.TYPE)) {
      return getHistogramAggregationBuilder((HistogramAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(FiltersAggBuilder.TYPE)) {
      return getFiltersAggregationBuilder((FiltersAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(TermsAggBuilder.TYPE)) {
      return getTermsAggregationBuilder((TermsAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(SumAggBuilder.TYPE)) {
      return getSumAggregationBuilder((SumAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(AvgAggBuilder.TYPE)) {
      return getAvgAggregationBuilder((AvgAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(MinAggBuilder.TYPE)) {
      return getMinAggregationBuilder((MinAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(MaxAggBuilder.TYPE)) {
      return getMaxAggregationBuilder((MaxAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(PercentilesAggBuilder.TYPE)) {
      return getPercentilesAggregationBuilder((PercentilesAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(UniqueCountAggBuilder.TYPE)) {
      return getUniqueCountAggregationBuilder((UniqueCountAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(ExtendedStatsAggBuilder.TYPE)) {
      return getExtendedStatsAggregationBuilder((ExtendedStatsAggBuilder) aggBuilder);
    } else {
      throw new IllegalArgumentException(
          String.format("Aggregation type %s not yet supported", aggBuilder.getType()));
    }
  }

  /**
   * Given an AggBuilder, will invoke the appropriate pipeline aggregation builder method to return
   * the abstract pipeline aggregation builder. This method is expected to be invoked from within
   * the bucket aggregation builders to compose a nested aggregation tree.@return
   */
  protected static AbstractPipelineAggregationBuilder<?> getPipelineAggregationBuilder(
      AggBuilder aggBuilder) {
    if (aggBuilder.getType().equals(MovingAvgAggBuilder.TYPE)) {
      return getMovingAverageAggregationBuilder((MovingAvgAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(CumulativeSumAggBuilder.TYPE)) {
      return getCumulativeSumAggregationBuilder((CumulativeSumAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(DerivativeAggBuilder.TYPE)) {
      return getDerivativeAggregationBuilder((DerivativeAggBuilder) aggBuilder);
    } else if (aggBuilder.getType().equals(MovingFunctionAggBuilder.TYPE)) {
      return getMovingFunctionAggregationBuilder((MovingFunctionAggBuilder) aggBuilder);
    } else {
      throw new IllegalArgumentException(
          String.format("PipelineAggregation type %s not yet supported", aggBuilder.getType()));
    }
  }

  /**
   * Determines if a given aggregation is of pipeline type, to allow for calling the appropriate
   * subAggregation builder step
   */
  protected static boolean isPipelineAggregation(AggBuilder aggBuilder) {
    List<String> pipelineAggregators =
        List.of(
            MovingAvgAggBuilder.TYPE,
            DerivativeAggBuilder.TYPE,
            CumulativeSumAggBuilder.TYPE,
            MovingFunctionAggBuilder.TYPE);
    return pipelineAggregators.contains(aggBuilder.getType());
  }

  /**
   * Given an SumAggBuilder returns a SumAggregationBuilder to be used in building aggregation tree
   */
  protected static SumAggregationBuilder getSumAggregationBuilder(SumAggBuilder builder) {
    SumAggregationBuilder sumAggregationBuilder =
        new SumAggregationBuilder(builder.getName()).field(builder.getField());

    if (builder.getScript() != null && !builder.getScript().isEmpty()) {
      sumAggregationBuilder.script(new Script(builder.getScript()));
    }

    if (builder.getMissing() != null) {
      sumAggregationBuilder.missing(builder.getMissing());
    }

    return sumAggregationBuilder;
  }

  /**
   * Given an AvgAggBuilder returns a AvgAggregationBuilder to be used in building aggregation tree
   */
  protected static AvgAggregationBuilder getAvgAggregationBuilder(AvgAggBuilder builder) {
    AvgAggregationBuilder avgAggregationBuilder =
        new AvgAggregationBuilder(builder.getName()).field(builder.getField());

    if (builder.getScript() != null && !builder.getScript().isEmpty()) {
      avgAggregationBuilder.script(new Script(builder.getScript()));
    }

    if (builder.getMissing() != null) {
      avgAggregationBuilder.missing(builder.getMissing());
    }

    return avgAggregationBuilder;
  }

  /**
   * Given an MinAggBuilder returns a MinAggregationBuilder to be used in building aggregation tree
   */
  protected static MinAggregationBuilder getMinAggregationBuilder(MinAggBuilder builder) {
    MinAggregationBuilder minAggregationBuilder =
        new MinAggregationBuilder(builder.getName()).field(builder.getField());

    if (builder.getScript() != null && !builder.getScript().isEmpty()) {
      minAggregationBuilder.script(new Script(builder.getScript()));
    }

    if (builder.getMissing() != null) {
      minAggregationBuilder.missing(builder.getMissing());
    }

    return minAggregationBuilder;
  }

  /**
   * Given an MaxAggBuilder returns a MaxAggregationBuilder to be used in building aggregation tree
   */
  protected static MaxAggregationBuilder getMaxAggregationBuilder(MaxAggBuilder builder) {
    MaxAggregationBuilder maxAggregationBuilder =
        new MaxAggregationBuilder(builder.getName()).field(builder.getField());

    if (builder.getScript() != null && !builder.getScript().isEmpty()) {
      maxAggregationBuilder.script(new Script(builder.getScript()));
    }

    if (builder.getMissing() != null) {
      maxAggregationBuilder.missing(builder.getMissing());
    }

    return maxAggregationBuilder;
  }

  /**
   * Given a UniqueCountAggBuilder, returns a CardinalityAggregationBuilder (aka UniqueCount) to be
   * used in building aggregation tree
   */
  protected static CardinalityAggregationBuilder getUniqueCountAggregationBuilder(
      UniqueCountAggBuilder builder) {

    CardinalityAggregationBuilder uniqueCountAggregationBuilder =
        new CardinalityAggregationBuilder(builder.getName()).field(builder.getField());

    if (builder.getPrecisionThreshold() != null) {
      uniqueCountAggregationBuilder.precisionThreshold(builder.getPrecisionThreshold());
    }

    if (builder.getMissing() != null) {
      uniqueCountAggregationBuilder.missing(builder.getMissing());
    }

    return uniqueCountAggregationBuilder;
  }

  /**
   * Given a ExtendedStatsAggBuilder, returns a ExtendedStatsAggregationBuilder to be used in
   * building aggregation tree. This returns all possible extended stats and it is up to the
   * requestor to filter to the desired fields
   */
  protected static ExtendedStatsAggregationBuilder getExtendedStatsAggregationBuilder(
      ExtendedStatsAggBuilder builder) {
    ExtendedStatsAggregationBuilder extendedStatsAggregationBuilder =
        new ExtendedStatsAggregationBuilder(builder.getName()).field(builder.getField());

    if (builder.getSigma() != null) {
      extendedStatsAggregationBuilder.sigma(builder.getSigma());
    }

    if (builder.getScript() != null && !builder.getScript().isEmpty()) {
      extendedStatsAggregationBuilder.script(new Script(builder.getScript()));
    }

    if (builder.getMissing() != null) {
      extendedStatsAggregationBuilder.missing(builder.getMissing());
    }

    return extendedStatsAggregationBuilder;
  }

  /**
   * Given a PercentilesAggBuilder, returns a PercentilesAggregationBuilder to be used in building
   * aggregation tree
   */
  protected static PercentilesAggregationBuilder getPercentilesAggregationBuilder(
      PercentilesAggBuilder builder) {
    PercentilesAggregationBuilder percentilesAggregationBuilder =
        new PercentilesAggregationBuilder(builder.getName())
            .field(builder.getField())
            .percentiles(builder.getPercentilesArray());

    if (builder.getScript() != null && !builder.getScript().isEmpty()) {
      percentilesAggregationBuilder.script(new Script(builder.getScript()));
    }

    if (builder.getMissing() != null) {
      percentilesAggregationBuilder.missing(builder.getMissing());
    }

    return percentilesAggregationBuilder;
  }

  /**
   * Given a MovingAvgAggBuilder, returns a MovAvgAggPipelineAggregation to be used in building the
   * aggregation tree.
   */
  protected static MovAvgPipelineAggregationBuilder getMovingAverageAggregationBuilder(
      MovingAvgAggBuilder builder) {
    MovAvgPipelineAggregationBuilder movAvgPipelineAggregationBuilder =
        new MovAvgPipelineAggregationBuilder(builder.getName(), builder.getBucketsPath());

    if (builder.getWindow() != null) {
      movAvgPipelineAggregationBuilder.window(builder.getWindow());
    }

    if (builder.getPredict() != null) {
      movAvgPipelineAggregationBuilder.predict(builder.getPredict());
    }

    movAvgPipelineAggregationBuilder.gapPolicy(BucketHelpers.GapPolicy.SKIP);

    //noinspection IfCanBeSwitch
    if (builder.getModel().equals("simple")) {
      movAvgPipelineAggregationBuilder.model(new SimpleModel());
    } else if (builder.getModel().equals("linear")) {
      movAvgPipelineAggregationBuilder.model(new LinearModel());
    } else if (builder.getModel().equals("ewma")) {
      MovAvgModel model = new EwmaModel();
      if (builder.getAlpha() != null) {
        model = new EwmaModel(builder.getAlpha());
      }
      movAvgPipelineAggregationBuilder.model(model);
      movAvgPipelineAggregationBuilder.minimize(builder.isMinimize());
    } else if (builder.getModel().equals("holt")) {
      MovAvgModel model = new HoltLinearModel();
      if (ObjectUtils.allNotNull(builder.getAlpha(), builder.getBeta())) {
        // both are non-null, use values provided instead of default
        model = new HoltLinearModel(builder.getAlpha(), builder.getBeta());
      } else if (ObjectUtils.anyNotNull(builder.getAlpha(), builder.getBeta())) {
        throw new IllegalArgumentException(
            String.format(
                "Both alpha and beta must be provided for HoltLinearMovingAvg if not using the default values [alpha:%s, beta:%s]",
                builder.getAlpha(), builder.getBeta()));
      }
      movAvgPipelineAggregationBuilder.model(model);
      movAvgPipelineAggregationBuilder.minimize(builder.isMinimize());
    } else if (builder.getModel().equals("holt_winters")) {
      // default as listed in the HoltWintersModel.java class
      // todo - this cannot be currently configured via Grafana, but may need to be an option?
      HoltWintersModel.SeasonalityType defaultSeasonalityType =
          HoltWintersModel.SeasonalityType.ADDITIVE;
      MovAvgModel model = new HoltWintersModel();
      if (ObjectUtils.allNotNull(
          builder.getAlpha(), builder.getBeta(), builder.getGamma(), builder.getPeriod())) {
        model =
            new HoltWintersModel(
                builder.getAlpha(),
                builder.getBeta(),
                builder.getGamma(),
                builder.getPeriod(),
                defaultSeasonalityType,
                builder.isPad());
      } else if (ObjectUtils.anyNotNull()) {
        throw new IllegalArgumentException(
            String.format(
                "Alpha, beta, gamma, period, and pad must be provided for HoltWintersMovingAvg if not using the default values [alpha:%s, beta:%s, gamma:%s, period:%s, pad:%s]",
                builder.getAlpha(),
                builder.getBeta(),
                builder.getGamma(),
                builder.getPeriod(),
                builder.isPad()));
      }
      movAvgPipelineAggregationBuilder.model(model);
      movAvgPipelineAggregationBuilder.minimize(builder.isMinimize());
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Model type of '%s' is not valid moving average model, must be one of ['simple', 'linear', 'ewma', 'holt', holt_winters']",
              builder.getModel()));
    }

    return movAvgPipelineAggregationBuilder;
  }

  /**
   * Given an MovingFunctionAggBuilder returns a MovFnPipelineAggregationBuilder to be used in
   * building aggregation tree
   */
  protected static MovFnPipelineAggregationBuilder getMovingFunctionAggregationBuilder(
      MovingFunctionAggBuilder builder) {
    MovFnPipelineAggregationBuilder movFnPipelineAggregationBuilder =
        new MovFnPipelineAggregationBuilder(
            builder.getName(),
            builder.getBucketsPath(),
            new Script(builder.getScript()),
            builder.getWindow());

    if (builder.getShift() != null) {
      movFnPipelineAggregationBuilder.setShift(builder.getShift());
    }

    return movFnPipelineAggregationBuilder;
  }

  /**
   * Given an CumulativeSumAggBuilder returns a CumulativeSumPipelineAggregationBuilder to be used
   * in building aggregation tree
   */
  protected static CumulativeSumPipelineAggregationBuilder getCumulativeSumAggregationBuilder(
      CumulativeSumAggBuilder builder) {
    CumulativeSumPipelineAggregationBuilder cumulativeSumPipelineAggregationBuilder =
        new CumulativeSumPipelineAggregationBuilder(builder.getName(), builder.getBucketsPath());

    if (builder.getFormat() != null && !builder.getFormat().isEmpty()) {
      cumulativeSumPipelineAggregationBuilder.format(builder.getFormat());
    }

    return cumulativeSumPipelineAggregationBuilder;
  }

  /**
   * Given a DerivativeAggBuilder returns a DerivativePipelineAggregationBuilder to be used in
   * building aggregation tree
   */
  protected static DerivativePipelineAggregationBuilder getDerivativeAggregationBuilder(
      DerivativeAggBuilder builder) {
    DerivativePipelineAggregationBuilder derivativePipelineAggregationBuilder =
        new DerivativePipelineAggregationBuilder(builder.getName(), builder.getBucketsPath());

    if (builder.getUnit() != null && !builder.getUnit().isEmpty()) {
      derivativePipelineAggregationBuilder.unit(builder.getUnit());
    }

    return derivativePipelineAggregationBuilder;
  }

  /**
   * Given an TermsAggBuilder returns a TermsAggregationBuilder to be used in building aggregation
   * tree
   */
  protected static TermsAggregationBuilder getTermsAggregationBuilder(TermsAggBuilder builder) {
    List<String> subAggNames =
        builder.getSubAggregations().stream()
            .map(subagg -> ((AggBuilderBase) subagg).getName())
            .collect(Collectors.toList());

    List<BucketOrder> order =
        builder.getOrder().entrySet().stream()
            .map(
                (entry) -> {
                  // todo - this potentially needs BucketOrder.compound support
                  boolean asc = !entry.getValue().equals("desc");
                  if (entry.getKey().equals("_count") || !subAggNames.contains(entry.getKey())) {
                    // we check to see if the requested key is in the sub-aggs; if not default to
                    // the count this is because when the Grafana plugin issues a request for
                    // Count agg (not Doc Count) it comes through as an agg request when the
                    // aggs are empty. This is fixed in later versions of the plugin, and will
                    // need to be ported to our fork as well.
                    return BucketOrder.count(asc);
                  } else if (entry.getKey().equals("_key") || entry.getKey().equals("_term")) {
                    // this is due to the fact that the kaldb plugin thinks this is ES < 6
                    // https://github.com/slackhq/slack-kaldb-app/blob/95b091184d5de1682c97586e271cbf2bbd7cc92a/src/datasource/QueryBuilder.ts#L55
                    return BucketOrder.key(asc);
                  } else {
                    return BucketOrder.aggregation(entry.getKey(), asc);
                  }
                })
            .collect(Collectors.toList());

    TermsAggregationBuilder termsAggregationBuilder =
        new TermsAggregationBuilder(builder.getName())
            .field(builder.getField())
            .executionHint("map")
            .minDocCount(builder.getMinDocCount())
            .size(builder.getSize())
            .order(order);

    if (builder.getMissing() != null) {
      termsAggregationBuilder.missing(builder.getMissing());
    }

    for (AggBuilder subAggregation : builder.getSubAggregations()) {
      if (isPipelineAggregation(subAggregation)) {
        termsAggregationBuilder.subAggregation(getPipelineAggregationBuilder(subAggregation));
      } else {
        termsAggregationBuilder.subAggregation(getAggregationBuilder(subAggregation));
      }
    }

    return termsAggregationBuilder;
  }

  /**
   * Given an FiltersAggBuilder returns a FiltersAggregationBuilder to be used in building
   * aggregation tree. Note this is different from a filter aggregation (single) as this takes a map
   * of keyed filters, but can be invoked with a single entry in the map.
   */
  protected static FiltersAggregationBuilder getFiltersAggregationBuilder(
      FiltersAggBuilder builder) {
    List<FiltersAggregator.KeyedFilter> keyedFilterList = new ArrayList<>();
    for (Map.Entry<String, FiltersAggBuilder.FilterAgg> stringFilterAggEntry :
        builder.getFilterAggMap().entrySet()) {
      FiltersAggBuilder.FilterAgg filterAgg = stringFilterAggEntry.getValue();
      keyedFilterList.add(
          new FiltersAggregator.KeyedFilter(
              stringFilterAggEntry.getKey(),
              new QueryStringQueryBuilder(filterAgg.getQueryString())
                  .lenient(true)
                  .analyzeWildcard(filterAgg.isAnalyzeWildcard())));
    }

    FiltersAggregationBuilder filtersAggregationBuilder =
        new FiltersAggregationBuilder(
            builder.getName(), keyedFilterList.toArray(new FiltersAggregator.KeyedFilter[0]));

    for (AggBuilder subAggregation : builder.getSubAggregations()) {
      if (isPipelineAggregation(subAggregation)) {
        filtersAggregationBuilder.subAggregation(getPipelineAggregationBuilder(subAggregation));
      } else {
        filtersAggregationBuilder.subAggregation(getAggregationBuilder(subAggregation));
      }
    }

    return filtersAggregationBuilder;
  }

  /**
   * Given an AutoDateHistogramAggBuilder returns a AutoDateHistogramAggBuilder to be used in
   * building aggregation tree
   */
  protected static AutoDateHistogramAggregationBuilder getAutoDateHistogramAggregationBuilder(
      AutoDateHistogramAggBuilder builder) {
    AutoDateHistogramAggregationBuilder autoDateHistogramAggregationBuilder =
        new AutoDateHistogramAggregationBuilder(builder.getName()).field(builder.getField());

    if (builder.getMinInterval() != null && !builder.getMinInterval().isEmpty()) {
      autoDateHistogramAggregationBuilder.setMinimumIntervalExpression(builder.getMinInterval());
    }

    if (builder.getNumBuckets() != null && builder.getNumBuckets() > 0) {
      autoDateHistogramAggregationBuilder.setNumBuckets(builder.getNumBuckets());
    }

    for (AggBuilder subAggregation : builder.getSubAggregations()) {
      if (isPipelineAggregation(subAggregation)) {
        autoDateHistogramAggregationBuilder.subAggregation(
            getPipelineAggregationBuilder(subAggregation));
      } else {
        autoDateHistogramAggregationBuilder.subAggregation(getAggregationBuilder(subAggregation));
      }
    }

    return autoDateHistogramAggregationBuilder;
  }

  /**
   * Given an DateHistogramAggBuilder returns a DateHistogramAggregationBuilder to be used in
   * building aggregation tree
   */
  protected static DateHistogramAggregationBuilder getDateHistogramAggregationBuilder(
      DateHistogramAggBuilder builder) {

    DateHistogramAggregationBuilder dateHistogramAggregationBuilder =
        new DateHistogramAggregationBuilder(builder.getName())
            .field(builder.getField())
            .minDocCount(builder.getMinDocCount())
            .fixedInterval(new DateHistogramInterval(builder.getInterval()));

    if (builder.getOffset() != null && !builder.getOffset().isEmpty()) {
      dateHistogramAggregationBuilder.offset(builder.getOffset());
    }

    if (builder.getFormat() != null && !builder.getFormat().isEmpty()) {
      // todo - this should be used when the field type is changed to date
      // dateHistogramAggregationBuilder.format(builder.getFormat());
    }

    if (builder.getZoneId() != null && !builder.getZoneId().isEmpty()) {
      dateHistogramAggregationBuilder.timeZone(ZoneId.of(builder.getZoneId()));
    }

    if (builder.getMinDocCount() == 0) {
      if (builder.getExtendedBounds() != null
          && builder.getExtendedBounds().containsKey("min")
          && builder.getExtendedBounds().containsKey("max")) {

        LongBounds longBounds =
            new LongBounds(
                builder.getExtendedBounds().get("min"), builder.getExtendedBounds().get("max"));
        dateHistogramAggregationBuilder.extendedBounds(longBounds);
      } else {
        // Minimum doc count _must_ be used with an extended bounds param
        // As per
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-histogram-aggregation.html#search-aggregations-bucket-histogram-aggregation-extended-bounds
        // "Using extended_bounds only makes sense when min_doc_count is 0 (the empty buckets will
        // never be returned if min_doc_count is greater than 0)."
        throw new IllegalArgumentException(
            "Extended bounds must be provided if using a min doc count");
      }
    }

    for (AggBuilder subAggregation : builder.getSubAggregations()) {
      if (isPipelineAggregation(subAggregation)) {
        dateHistogramAggregationBuilder.subAggregation(
            getPipelineAggregationBuilder(subAggregation));
      } else {
        dateHistogramAggregationBuilder.subAggregation(getAggregationBuilder(subAggregation));
      }
    }

    return dateHistogramAggregationBuilder;
  }

  /**
   * Given an HistogramAggBuilder returns a HistogramAggregationBuilder to be used in building
   * aggregation tree
   */
  protected static HistogramAggregationBuilder getHistogramAggregationBuilder(
      HistogramAggBuilder builder) {

    HistogramAggregationBuilder histogramAggregationBuilder =
        new HistogramAggregationBuilder(builder.getName())
            .field(builder.getField())
            .minDocCount(builder.getMinDocCount())
            .interval(builder.getIntervalDouble());

    for (AggBuilder subAggregation : builder.getSubAggregations()) {
      if (isPipelineAggregation(subAggregation)) {
        histogramAggregationBuilder.subAggregation(getPipelineAggregationBuilder(subAggregation));
      } else {
        histogramAggregationBuilder.subAggregation(getAggregationBuilder(subAggregation));
      }
    }

    return histogramAggregationBuilder;
  }
}
