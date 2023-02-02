package com.slack.kaldb.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.slack.kaldb.histogram.FixedIntervalHistogramImpl;
import com.slack.kaldb.histogram.Histogram;
import com.slack.kaldb.histogram.HistogramBucket;
import com.slack.kaldb.histogram.NoOpHistogramImpl;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogMessage.SystemField;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.logstore.OpensearchShim;
import com.slack.kaldb.logstore.search.queryparser.KaldbQueryParser;
import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import com.slack.kaldb.util.JsonUtil;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.common.io.stream.InputStreamStreamInput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.slack.kaldb.util.ArgValidationUtils.ensureNonEmptyString;
import static com.slack.kaldb.util.ArgValidationUtils.ensureNonNullString;
import static com.slack.kaldb.util.ArgValidationUtils.ensureTrue;

/*
 * A wrapper around lucene that helps us search a single index containing logs.
 * TODO: Add template type to this class definition.
 */
public class LogIndexSearcherImpl implements LogIndexSearcher<LogMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(LogIndexSearcherImpl.class);

  private final SearcherManager searcherManager;
  private final StandardAnalyzer analyzer;

  private final ConcurrentHashMap<String, LuceneFieldDef> chunkSchema;

  @VisibleForTesting
  public static SearcherManager searcherManagerFromPath(Path path) throws IOException {
    MMapDirectory directory = new MMapDirectory(path);
    return new SearcherManager(directory, null);
  }

  // todo - this is not needed once this data is on the snapshot
  public static int getNumDocs(Path path) throws IOException {
    MMapDirectory directory = new MMapDirectory(path);
    DirectoryReader directoryReader = DirectoryReader.open(directory);
    int numDocs = directoryReader.numDocs();
    directoryReader.close();
    return numDocs;
  }

  public LogIndexSearcherImpl(
      SearcherManager searcherManager, ConcurrentHashMap<String, LuceneFieldDef> chunkSchema) {
    this.searcherManager = searcherManager;
    this.analyzer = new StandardAnalyzer();
    this.chunkSchema = chunkSchema;
  }

  // Lucene's query parsers are not thread safe. So, create a new one for every request.
  private QueryParser buildQueryParser() {
    return new KaldbQueryParser(SystemField.ALL.fieldName, analyzer, chunkSchema);
  }

  public SearchResult<LogMessage> search(
      String dataset,
      String queryStr,
      long startTimeMsEpoch,
      long endTimeMsEpoch,
      int howMany,
      int bucketCount) {

    ensureNonEmptyString(dataset, "dataset should be a non-empty string");
    ensureNonNullString(queryStr, "query should be a non-empty string");
    ensureTrue(startTimeMsEpoch >= 0, "start time should be non-negative value");
    ensureTrue(startTimeMsEpoch < endTimeMsEpoch, "end time should be greater than start time");
    ensureTrue(howMany >= 0, "hits requested should not be negative.");
    ensureTrue(bucketCount >= 0, "bucket count should not be negative.");
    ensureTrue(howMany > 0 || bucketCount > 0, "Hits or histogram should be requested.");

    ScopedSpan span = Tracing.currentTracer().startScopedSpan("LogIndexSearcherImpl.search");
    span.tag("dataset", dataset);
    span.tag("queryStr", queryStr);
    span.tag("startTimeMsEpoch", String.valueOf(startTimeMsEpoch));
    span.tag("endTimeMsEpoch", String.valueOf(endTimeMsEpoch));
    span.tag("howMany", String.valueOf(howMany));
    span.tag("bucketCount", String.valueOf(bucketCount));

    Stopwatch elapsedTime = Stopwatch.createStarted();
    try {
      Query query = buildQuery(span, dataset, queryStr, startTimeMsEpoch, endTimeMsEpoch);

      // Acquire an index searcher from searcher manager.
      // This is a useful optimization for indexes that are static.
      IndexSearcher searcher = searcherManager.acquire();
      try {
        List<LogMessage> results;
        InternalAutoDateHistogram histogram = null;

        if (howMany > 0) {
          CollectorManager<TopFieldCollector, TopFieldDocs> topFieldCollector =
              buildTopFieldCollector(howMany, bucketCount > 0 ? Integer.MAX_VALUE : howMany);
          MultiCollectorManager collectorManager;
          if (bucketCount > 0) {
            collectorManager = new MultiCollectorManager(topFieldCollector, OpensearchShim.getCollectorManager(bucketCount, startTimeMsEpoch, endTimeMsEpoch));
          } else {
            collectorManager = new MultiCollectorManager(topFieldCollector);
          }
          Object[] collector = searcher.search(query, collectorManager);

          ScoreDoc[] hits = ((TopFieldDocs) collector[0]).scoreDocs;
          results = new ArrayList<>(hits.length);
          for (ScoreDoc hit : hits) {
            results.add(buildLogMessage(searcher, hit));
          }
          if (bucketCount > 0) {
            histogram = ((InternalAutoDateHistogram) collector[1]);
          }
        } else {
          results = Collections.emptyList();
          Object[] collector = searcher.search(query, new MultiCollectorManager(OpensearchShim.getCollectorManager(bucketCount, startTimeMsEpoch, endTimeMsEpoch)));
          histogram = ((InternalAutoDateHistogram) collector[0]);
        }

        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
            Arrays.asList(
                new NamedWriteableRegistry.Entry(AggregationBuilder.class, AutoDateHistogramAggregationBuilder.NAME, AutoDateHistogramAggregationBuilder::new),
                new NamedWriteableRegistry.Entry(InternalAggregation.class, AutoDateHistogramAggregationBuilder.NAME, InternalAutoDateHistogram::new),
                new NamedWriteableRegistry.Entry(AggregationBuilder.class, ValueCountAggregationBuilder.NAME, ValueCountAggregationBuilder::new),
                new NamedWriteableRegistry.Entry(InternalAggregation.class, ValueCountAggregationBuilder.NAME, InternalValueCount::new),
                new NamedWriteableRegistry.Entry(DocValueFormat.class, DocValueFormat.BOOLEAN.getWriteableName(), in -> DocValueFormat.BOOLEAN),
                new NamedWriteableRegistry.Entry(DocValueFormat.class, DocValueFormat.DateTime.NAME, DocValueFormat.DateTime::new),
                new NamedWriteableRegistry.Entry(DocValueFormat.class, DocValueFormat.Decimal.NAME, DocValueFormat.Decimal::new),
                new NamedWriteableRegistry.Entry(DocValueFormat.class, DocValueFormat.GEOHASH.getWriteableName(), in -> DocValueFormat.GEOHASH),
                new NamedWriteableRegistry.Entry(DocValueFormat.class, DocValueFormat.GEOTILE.getWriteableName(), in -> DocValueFormat.GEOTILE),
                new NamedWriteableRegistry.Entry(DocValueFormat.class, DocValueFormat.IP.getWriteableName(), in -> DocValueFormat.IP),
                new NamedWriteableRegistry.Entry(DocValueFormat.class, DocValueFormat.RAW.getWriteableName(), in -> DocValueFormat.RAW),
                new NamedWriteableRegistry.Entry(DocValueFormat.class, DocValueFormat.BINARY.getWriteableName(), in -> DocValueFormat.BINARY),
                new NamedWriteableRegistry.Entry(DocValueFormat.class, DocValueFormat.UNSIGNED_LONG_SHIFTED.getWriteableName(), in -> DocValueFormat.UNSIGNED_LONG_SHIFTED)
            )
        );

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        StreamOutput streamOutput = new OutputStreamStreamOutput(byteArrayOutputStream);
        histogram.writeTo(streamOutput);

        InputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        StreamInput streamInput = new InputStreamStreamInput(inputStream);
        NamedWriteableAwareStreamInput namedWriteableAwareStreamInput = new NamedWriteableAwareStreamInput(streamInput, namedWriteableRegistry);

        //FilterStreamInput fs = new FilterStreamInput(streamInput);
        InternalAutoDateHistogram reconstructed = new InternalAutoDateHistogram(namedWriteableAwareStreamInput);

        // todo - this is a temp test
        //  the returned buckets aren't consistent across nodes, so we need to coerce these for now since
        //  the query node can't handle these quite correctly - this will result in buckets that aren't quite accurate
        //  especially at the start and end
        List<HistogramBucket> buckets = FixedIntervalHistogramImpl.makeHistogram(startTimeMsEpoch, endTimeMsEpoch, bucketCount);
        long totalCount = results.size();
        if (histogram != null) {
          totalCount = histogram.getBuckets().stream().collect(Collectors.summarizingLong(InternalAutoDateHistogram.Bucket::getDocCount)).getSum();
          for (int i = 0; i < histogram.getBuckets().size(); i++) {
            InternalAutoDateHistogram.Bucket bucket = histogram.getBuckets().get(i);
            long key = Long.valueOf(bucket.getKeyAsString());
            buckets.stream().forEach(histogramBucket -> {
              if (histogramBucket.getHigh() >= key && key >= histogramBucket.getLow()) {
                histogramBucket.increment(bucket.getDocCount());
              }
            });
          }
        }

        elapsedTime.stop();
        return new SearchResult<>(
            results,
            elapsedTime.elapsed(TimeUnit.MICROSECONDS),
            totalCount,
            buckets,
            0,
            0,
            1,
            1);
      } finally {
        searcherManager.release(searcher);
      }
    } catch (ParseException e) {
      span.error(e);
      throw new IllegalArgumentException("Unable to parse query string: " + queryStr, e);
      // TODO: Return Empty search result?
    } catch (IOException e) {
      span.error(e);
      throw new IllegalArgumentException("Failed to acquire an index searcher.", e);
    } finally {
      span.finish();
    }
  }

  private LogMessage buildLogMessage(IndexSearcher searcher, ScoreDoc hit) {
    String s = "";
    try {
      s = searcher.doc(hit.doc).get(SystemField.SOURCE.fieldName);
      LogWireMessage wireMessage = JsonUtil.read(s, LogWireMessage.class);
      return new LogMessage(
          wireMessage.getIndex(), wireMessage.getType(), wireMessage.id, wireMessage.source);
    } catch (Exception e) {
      throw new IllegalStateException("Error fetching and parsing a result from index: " + s, e);
    }
  }

  /**
   * Builds a top field collector for the requested amount of results, with the option to set the
   * totalHitsThreshold. If the totalHitsThreshold is set to Integer.MAX_VALUE it will force a
   * ScoreMode.COMPLETE, iterating over all documents at the expense of a longer query time. This
   * value can be set to equal howMany to allow early exiting (ScoreMode.TOP_SCORES), but should
   * only be done when all collectors are tolerant of an early exit.
   */
  private CollectorManager<TopFieldCollector, TopFieldDocs> buildTopFieldCollector(
      int howMany, int totalHitsThreshold) {
    if (howMany > 0) {
      SortField sortField = new SortField(SystemField.TIME_SINCE_EPOCH.fieldName, Type.LONG, true);
      return TopFieldCollector.createSharedManager(
          new Sort(sortField), howMany, null, totalHitsThreshold);
    } else {
      return null;
    }
  }

  private CollectorManager<StatsCollector, Histogram> buildStatsCollector(
      int bucketCount, long startTimeMsEpoch, long endTimeMsEpoch) {
    Histogram histogram =
        bucketCount > 0
            ? new FixedIntervalHistogramImpl(startTimeMsEpoch, endTimeMsEpoch, bucketCount)
            : new NoOpHistogramImpl();

    return new CollectorManager<>() {
      @Override
      public StatsCollector newCollector() {
        return new StatsCollector(histogram);
      }

      @Override
      public Histogram reduce(Collection<StatsCollector> collectors) {
        Histogram histogram = null;
        for (StatsCollector collector : collectors) {
          if (histogram == null) {
            histogram = collector.getHistogram();
          } else {
            histogram.mergeHistogram(collector.getHistogram().getBuckets());
          }
        }
        return histogram;
      }
    };
  }

  private Query buildQuery(
      ScopedSpan span, String dataset, String queryStr, long startTimeMsEpoch, long endTimeMsEpoch)
      throws ParseException {
    Builder queryBuilder = new Builder();

    // todo - we currently do not enforce searching against an dataset name, as we do not support
    //  multi-tenancy yet - see https://github.com/slackhq/kaldb/issues/223. Once index filtering
    //  is support at snapshot/query layer this should be re-enabled as appropriate.
    // queryBuilder.add(new TermQuery(new Term(SystemField.INDEX.fieldName, dataset)),
    // Occur.MUST);
    queryBuilder.add(
        LongPoint.newRangeQuery(
            SystemField.TIME_SINCE_EPOCH.fieldName, startTimeMsEpoch, endTimeMsEpoch),
        Occur.MUST);
    if (queryStr.length() > 0) {
      queryBuilder.add(buildQueryParser().parse(queryStr), Occur.MUST);
    }
    BooleanQuery query = queryBuilder.build();
    span.tag("lucene_query", query.toString());
    span.tag("lucene_query_num_clauses", Integer.toString(query.clauses().size()));
    return query;
  }

  @Override
  public void close() {
    try {
      searcherManager.close();
    } catch (IOException e) {
      LOG.error("Encountered error closing searcher manager", e);
    }
  }
}
