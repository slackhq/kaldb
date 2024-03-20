package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.COMMITS_TIMER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.REFRESHES_TIMER;
import static com.slack.kaldb.testlib.MessageUtil.TEST_DATASET_NAME;
import static com.slack.kaldb.testlib.MessageUtil.TEST_SOURCE_LONG_PROPERTY;
import static com.slack.kaldb.testlib.MessageUtil.TEST_SOURCE_STRING_PROPERTY;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.MetricsUtil.getTimerCount;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherExtension.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import brave.Tracing;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.aggregations.AvgAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.ExtendedStatsAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.FiltersAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.MaxAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.MinAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.MovingAvgAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.SumAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.TermsAggBuilder;
import com.slack.kaldb.proto.schema.Schema;
import com.slack.kaldb.testlib.SpanUtil;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherExtension;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.bucket.filter.InternalFilters;
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalExtendedStats;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;

public class LogIndexSearcherImplTest {

  @RegisterExtension
  public TemporaryLogStoreAndSearcherExtension strictLogStore =
      new TemporaryLogStoreAndSearcherExtension(true);

  @RegisterExtension
  public TemporaryLogStoreAndSearcherExtension strictLogStoreWithoutFts =
      new TemporaryLogStoreAndSearcherExtension(false);

  public LogIndexSearcherImplTest() throws IOException {}

  @BeforeAll
  public static void beforeClass() {
    Tracing.newBuilder().build();
  }

  private void loadTestData(Instant time) {
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(3, "apple baby", time.plusSeconds(2)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(4, "car", time.plusSeconds(3)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(5, "apple baby car", time.plusSeconds(4)));
    // when we enable multi-tenancy, we can add messages to different indices

    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
  }

  @Test
  public void testTimeBoundSearch() {
    Instant time = Instant.now();
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, time.plusSeconds(100)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

    // Start inclusive.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "Message1",
                    time.toEpochMilli(),
                    time.plusSeconds(10).toEpochMilli(),
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(1);

    // Extended range still only picking one element.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "Message1",
                    time.minusSeconds(1).toEpochMilli(),
                    time.plusSeconds(90).toEpochMilli(),
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(1);

    // Both ranges are inclusive.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "_id:Message1 OR Message2",
                    time.toEpochMilli(),
                    time.plusSeconds(100).toEpochMilli(),
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(2);

    // Extended range to pick up both events
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "_id:Message1 OR Message2",
                    time.minusSeconds(1).toEpochMilli(),
                    time.plusSeconds(1000).toEpochMilli(),
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(2);
  }

  @Test
  @Disabled // todo - re-enable when multi-tenancy is supported - slackhq/kaldb/issues/223
  public void testIndexBoundSearch() {
    Instant time = Instant.ofEpochSecond(1593365471);
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, time));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "idx",
                    "test1",
                    time.minusSeconds(1).toEpochMilli(),
                    time.plusSeconds(10).toEpochMilli(),
                    100,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(1);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "idx1",
                    "test1",
                    time.toEpochMilli(),
                    time.plusSeconds(10).toEpochMilli(),
                    100,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(1);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "idx12",
                    "test1",
                    time.toEpochMilli(),
                    time.plusSeconds(10).toEpochMilli(),
                    100,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(0);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    "idx1",
                    "test",
                    time.toEpochMilli(),
                    time.plusSeconds(10).toEpochMilli(),
                    100,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(0);
  }

  @Test
  public void testSearchMultipleItemsAndIndices() {
    Instant time = Instant.now();
    loadTestData(time);
    SearchResult<LogMessage> babies =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "Message1",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(babies.hits.size()).isEqualTo(1);

    InternalDateHistogram histogram =
        (InternalDateHistogram) Objects.requireNonNull(babies.internalAggregation);
    assertThat(histogram.getBuckets().size()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
  }

  @Test
  public void testAllQueryWithFullTextSearchEnabled() {
    Instant time = Instant.now();

    Trace.KeyValue customField =
        Trace.KeyValue.newBuilder()
            .setVStr("value")
            .setKey("customField")
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .build();

    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time, List.of(customField)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> termQuery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "customField:value",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(termQuery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> noTermStrQuery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "value",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(noTermStrQuery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> noTermNumericQuery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "Message1",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(noTermNumericQuery.hits.size()).isEqualTo(1);
  }

  @Test
  public void testAllQueryWithFullTextSearchDisabled() {
    Instant time = Instant.now();
    Trace.KeyValue customField =
        Trace.KeyValue.newBuilder()
            .setVStr("value")
            .setKey("customField")
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .build();
    strictLogStoreWithoutFts.logStore.addMessage(
        SpanUtil.makeSpan(1, "apple", time, List.of(customField)));
    strictLogStoreWithoutFts.logStore.commit();
    strictLogStoreWithoutFts.logStore.refresh();

    SearchResult<LogMessage> termQuery =
        strictLogStoreWithoutFts.logSearcher.search(
            TEST_DATASET_NAME,
            "customField:value",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(termQuery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> noTermStrQuery =
        strictLogStoreWithoutFts.logSearcher.search(
            TEST_DATASET_NAME,
            "value",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(noTermStrQuery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> noTermNumericQuery =
        strictLogStoreWithoutFts.logSearcher.search(
            TEST_DATASET_NAME,
            "1",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(noTermNumericQuery.hits.size()).isEqualTo(1);
  }

  @Test
  public void testExistsQuery() {
    Instant time = Instant.now();
    Trace.KeyValue customField =
        Trace.KeyValue.newBuilder()
            .setVStr("value")
            .setKey("customField")
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .build();
    Trace.KeyValue customField1 =
        Trace.KeyValue.newBuilder()
            .setVStr("value")
            .setKey("customField1")
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .build();
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time, List.of(customField)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, "apple", time, List.of(customField1)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> exists =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "_exists_:customField",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(exists.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> termQuery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "customField:value",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(termQuery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> notExists =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "_exists_:foo",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(notExists.hits.size()).isEqualTo(0);
  }

  @Test
  public void testRangeQuery() {
    Instant time = Instant.now();

    Trace.KeyValue valTag1 =
        Trace.KeyValue.newBuilder()
            .setKey("val")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .setVInt32(1)
            .build();
    Trace.KeyValue valTag2 =
        Trace.KeyValue.newBuilder()
            .setKey("val")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .setVInt32(2)
            .build();
    Trace.KeyValue valTag3 =
        Trace.KeyValue.newBuilder()
            .setKey("val")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .setVInt32(3)
            .build();
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time, List.of(valTag1)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, "bear", time, List.of(valTag2)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(3, "car", time, List.of(valTag3)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> rangeBoundInclusive =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "val:[1 TO 3]",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(rangeBoundInclusive.hits.size()).isEqualTo(3);

    SearchResult<LogMessage> rangeBoundExclusive =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "val:{1 TO 3}",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(rangeBoundExclusive.hits.size()).isEqualTo(1);
  }

  @Test
  public void testQueryParsingFieldTypes() {
    Instant time = Instant.now();

    Trace.KeyValue boolTag =
        Trace.KeyValue.newBuilder()
            .setVBool(true)
            .setKey("boolval")
            .setFieldType(Schema.SchemaFieldType.BOOLEAN)
            .build();

    Trace.KeyValue intTag =
        Trace.KeyValue.newBuilder()
            .setVInt32(1)
            .setKey("intval")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();

    Trace.KeyValue longTag =
        Trace.KeyValue.newBuilder()
            .setVInt64(2L)
            .setKey("longval")
            .setFieldType(Schema.SchemaFieldType.LONG)
            .build();

    Trace.KeyValue floatTag =
        Trace.KeyValue.newBuilder()
            .setVFloat32(3F)
            .setKey("floatval")
            .setFieldType(Schema.SchemaFieldType.FLOAT)
            .build();

    Trace.KeyValue doubleTag =
        Trace.KeyValue.newBuilder()
            .setVFloat64(4D)
            .setKey("doubleval")
            .setFieldType(Schema.SchemaFieldType.DOUBLE)
            .build();

    Trace.Span span =
        SpanUtil.makeSpan(1, "apple", time, List.of(boolTag, intTag, longTag, floatTag, doubleTag));
    strictLogStore.logStore.addMessage(span);
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> boolquery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "boolval:true",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(boolquery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> intquery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "intval:1",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(intquery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> longquery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "longval:2",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(longquery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> floatquery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "floatval:3",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(floatquery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> doublequery =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "doubleval:4",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(doublequery.hits.size()).isEqualTo(1);
  }

  @Test
  public void testTopKQuery() {
    Instant time = Instant.now();
    loadTestData(time);

    SearchResult<LogMessage> apples =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "apple",
            time.toEpochMilli(),
            time.plusSeconds(100).toEpochMilli(),
            2,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(apples.hits.stream().map(m -> m.getId()).collect(Collectors.toList()))
        .isEqualTo(Arrays.asList("Message5", "Message3"));
    assertThat(apples.hits.size()).isEqualTo(2);

    InternalDateHistogram histogram =
        (InternalDateHistogram) Objects.requireNonNull(apples.internalAggregation);

    assertThat(histogram.getBuckets().size()).isEqualTo(3);
    assertThat(histogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(1).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(2).getDocCount()).isEqualTo(1);
  }

  @Test
  public void testSearchMultipleCommits() {
    Instant time = Instant.now();

    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, "apple baby", time.plusSeconds(2)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> baby =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "baby",
            time.toEpochMilli(),
            time.plusSeconds(10).toEpochMilli(),
            2,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(baby.hits.size()).isEqualTo(1);
    assertThat(baby.hits.get(0).getId()).isEqualTo("Message2");
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

    // Add car but don't commit. So, no results for car.
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(3, "car", time.plusSeconds(3)));

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

    strictLogStore.logSearcher.search(
        TEST_DATASET_NAME,
        "car",
        time.toEpochMilli(),
        time.plusSeconds(10).toEpochMilli(),
        2,
        new DateHistogramAggBuilder("1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));

    // Commit but no refresh. Item is still not available for search.
    strictLogStore.logStore.commit();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(2);

    strictLogStore.logSearcher.search(
        TEST_DATASET_NAME,
        "car",
        time.toEpochMilli(),
        time.plusSeconds(10).toEpochMilli(),
        2,
        new DateHistogramAggBuilder("1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));

    // Car can be searched after refresh.
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(2);

    strictLogStore.logSearcher.search(
        TEST_DATASET_NAME,
        "car",
        time.toEpochMilli(),
        time.plusSeconds(10).toEpochMilli(),
        2,
        new DateHistogramAggBuilder("1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));

    // Add another message to search, refresh but don't commit.
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(4, "apple baby car", time.plusSeconds(4)));
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(4);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(2);

    // Item shows up in search without commit.
    SearchResult<LogMessage> babies =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "baby",
            time.toEpochMilli(),
            time.plusSeconds(10).toEpochMilli(),
            2,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(babies.hits.size()).isEqualTo(2);
    assertThat(babies.hits.stream().map(m -> m.getId()).collect(Collectors.toList()))
        .isEqualTo(Arrays.asList("Message4", "Message2"));

    // Commit now
    strictLogStore.logStore.commit();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(4);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(3);
  }

  @Test
  public void testFullIndexSearch() {
    loadTestData(Instant.now());

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "",
            0L,
            MAX_TIME,
            1000,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    InternalDateHistogram histogram =
        (InternalDateHistogram) Objects.requireNonNull(allIndexItems.internalAggregation);
    // assertThat(histogram.getTargetBuckets()).isEqualTo(1);

    assertThat(histogram.getBuckets().size()).isEqualTo(4);
    assertThat(histogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(1).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(2).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(3).getDocCount()).isEqualTo(1);
  }

  @Test
  public void testAggregationWithScripting() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> scriptNull =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "",
            0L,
            MAX_TIME,
            1000,
            new AvgAggBuilder("1", TEST_SOURCE_LONG_PROPERTY, 0, null));
    assertThat(((InternalAvg) scriptNull.internalAggregation).value()).isEqualTo(3.25);

    SearchResult<LogMessage> scriptEmpty =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "",
            0L,
            MAX_TIME,
            1000,
            new AvgAggBuilder("1", TEST_SOURCE_LONG_PROPERTY, 0, ""));
    assertThat(((InternalAvg) scriptEmpty.internalAggregation).value()).isEqualTo(3.25);

    SearchResult<LogMessage> scripted =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "",
            0L,
            MAX_TIME,
            1000,
            new AvgAggBuilder("1", TEST_SOURCE_LONG_PROPERTY, 0, "return 9;"));
    assertThat(((InternalAvg) scripted.internalAggregation).value()).isEqualTo(9);
  }

  @Test
  public void testFilterAggregations() {
    Instant time = Instant.now();
    loadTestData(time);

    SearchResult<LogMessage> scriptNull =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "",
            0L,
            MAX_TIME,
            1000,
            new FiltersAggBuilder(
                "1",
                List.of(),
                Map.of(
                    "foo",
                        new FiltersAggBuilder.FilterAgg(
                            String.format(
                                "%s:<=%s",
                                LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                                time.plusSeconds(2).toEpochMilli()),
                            true),
                    "bar",
                        new FiltersAggBuilder.FilterAgg(
                            String.format(
                                "%s:>%s",
                                LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                                time.plusSeconds(2).toEpochMilli()),
                            true))));

    assertThat(((InternalFilters) scriptNull.internalAggregation).getBuckets().size()).isEqualTo(2);
    assertThat(((InternalFilters) scriptNull.internalAggregation).getBuckets().get(0).getDocCount())
        .isEqualTo(2);
    assertThat(((InternalFilters) scriptNull.internalAggregation).getBuckets().get(0).getKey())
        .isIn(List.of("foo", "bar"));
    assertThat(((InternalFilters) scriptNull.internalAggregation).getBuckets().get(1).getDocCount())
        .isEqualTo(2);
    assertThat(((InternalFilters) scriptNull.internalAggregation).getBuckets().get(1).getKey())
        .isIn(List.of("foo", "bar"));
    assertThat(((InternalFilters) scriptNull.internalAggregation).getBuckets().get(0).getKey())
        .isNotEqualTo(
            ((InternalFilters) scriptNull.internalAggregation).getBuckets().get(1).getKey());
  }

  @Test
  public void testFullIndexSearchForMinAgg() {
    Instant time = Instant.now();
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "",
            0L,
            MAX_TIME,
            1000,
            new MinAggBuilder(
                "test", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "0", null));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    InternalMin internalMin =
        (InternalMin) Objects.requireNonNull(allIndexItems.internalAggregation);

    assertThat(Double.valueOf(internalMin.getValue()).longValue()).isEqualTo(time.toEpochMilli());
  }

  @Test
  public void testFullIndexSearchForMaxAgg() {
    Instant time = Instant.now();
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "",
            0L,
            MAX_TIME,
            1000,
            new MaxAggBuilder(
                "test", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "0", null));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    InternalMax internalMax =
        (InternalMax) Objects.requireNonNull(allIndexItems.internalAggregation);

    // 4 seconds because of test data
    assertThat(Double.valueOf(internalMax.getValue()).longValue())
        .isEqualTo(time.plus(4, ChronoUnit.SECONDS).toEpochMilli());
  }

  @Test
  public void testFullIndexSearchForSumAgg() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "",
            0L,
            MAX_TIME,
            1000,
            new SumAggBuilder("test", TEST_SOURCE_LONG_PROPERTY, "0", null));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    InternalSum internalSum =
        (InternalSum) Objects.requireNonNull(allIndexItems.internalAggregation);

    // 1, 3, 4, 5
    assertThat(internalSum.getValue()).isEqualTo(13);
  }

  @Test
  public void testFullIndexSearchForExtendedStatsAgg() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "",
            0L,
            MAX_TIME,
            1000,
            new ExtendedStatsAggBuilder("test", TEST_SOURCE_LONG_PROPERTY, "0", null, null));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    InternalExtendedStats internalExtendedStats =
        (InternalExtendedStats) Objects.requireNonNull(allIndexItems.internalAggregation);

    // 1, 3, 4, 5
    assertThat(internalExtendedStats).isNotNull();
    assertThat(internalExtendedStats.getCount()).isEqualTo(4);
    assertThat(internalExtendedStats.getMax()).isEqualTo(5);
    assertThat(internalExtendedStats.getMin()).isEqualTo(1);
    assertThat(internalExtendedStats.getSum()).isEqualTo(13);
    assertThat(internalExtendedStats.getAvg()).isEqualTo(3.25);
    assertThat(internalExtendedStats.getSumOfSquares()).isEqualTo(51);
    assertThat(internalExtendedStats.getVariance()).isEqualTo(2.1875);
  }

  @Test
  public void testTermsAggregation() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "",
            0L,
            MAX_TIME,
            1000,
            new TermsAggBuilder(
                "1",
                List.of(),
                TEST_SOURCE_STRING_PROPERTY,
                "foo",
                10,
                0,
                Map.of("_count", "asc")));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    StringTerms stringTerms = (StringTerms) allIndexItems.internalAggregation;
    assertThat(stringTerms.getBuckets().size()).isEqualTo(4);

    List<String> bucketKeys =
        stringTerms.getBuckets().stream()
            .map(bucket -> (String) bucket.getKey())
            .collect(Collectors.toList());
    assertThat(bucketKeys.contains("String-1")).isTrue();
    assertThat(bucketKeys.contains("String-3")).isTrue();
    assertThat(bucketKeys.contains("String-4")).isTrue();
    assertThat(bucketKeys.contains("String-5")).isTrue();
  }

  @Test
  public void testPipelineAggregation() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchQuery query =
        new SearchQuery(
            TEST_DATASET_NAME,
            "",
            1593365471000L,
            1593365471000L + 5000L,
            1000,
            new DateHistogramAggBuilder(
                "histo",
                LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                "1s",
                null,
                null,
                0,
                "epoch_ms",
                Map.of("min", 1593365471000L, "max", 1593365471000L + 5000L),
                List.of(
                    new AvgAggBuilder(
                        "avgTimestamp",
                        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                        null,
                        null),
                    new MovingAvgAggBuilder("movAvgCount", "_count", "simple", 2, 1))),
            List.of());

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            query.dataset,
            query.queryStr,
            query.startTimeEpochMs,
            query.endTimeEpochMs,
            query.howMany,
            query.aggBuilder);

    SearchResultAggregatorImpl<LogMessage> aggregator = new SearchResultAggregatorImpl<>(query);
    SearchResult<LogMessage> allIndexItemsFinal =
        aggregator.aggregate(List.of(allIndexItems), true);
    InternalDateHistogram dateHistogram =
        (InternalDateHistogram) allIndexItemsFinal.internalAggregation;

    assertThat(dateHistogram.getBuckets().size()).isEqualTo(8);
    // we collect to a set here, because opensearch doubles up the pipeline aggregators for some
    // reason
    assertThat(
            dateHistogram.getBuckets().get(0).getAggregations().asList().stream()
                .map(Aggregation::getName)
                .collect(Collectors.toSet()))
        .containsExactly("avgTimestamp");
    for (int i = 1; i < 6; i++) {
      assertThat(
              dateHistogram.getBuckets().get(i).getAggregations().asList().stream()
                  .map(Aggregation::getName)
                  .collect(Collectors.toSet()))
          .containsExactly("avgTimestamp", "movAvgCount");
    }
    assertThat(
            dateHistogram.getBuckets().get(6).getAggregations().asList().stream()
                .map(Aggregation::getName)
                .collect(Collectors.toSet()))
        .containsExactly("movAvgCount");
    assertThat(
            dateHistogram.getBuckets().get(7).getAggregations().asList().stream()
                .map(Aggregation::getName)
                .collect(Collectors.toSet()))
        .containsExactly("movAvgCount");
  }

  @Test
  public void testTermsAggregationMissingValues() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "",
            0L,
            MAX_TIME,
            1000,
            new TermsAggBuilder(
                "1", List.of(), "thisFieldDoesNotExist", "foo", 10, 0, Map.of("_count", "asc")));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    StringTerms stringTerms = (StringTerms) allIndexItems.internalAggregation;
    assertThat(stringTerms.getBuckets().size()).isEqualTo(1);
    assertThat(stringTerms.getBuckets().get(0).getKey()).isEqualTo("foo");
  }

  @Test
  public void testFullTextSearch() {
    Instant time = Instant.ofEpochSecond(1593365471);

    Trace.KeyValue customField =
        Trace.KeyValue.newBuilder()
            .setVInt32(1234)
            .setKey("field1")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();

    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time, List.of(customField)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
    // Search using _all field.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "_all:apple",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(1);
    // Default all field search.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "Message1",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(1);

    strictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, "apple baby", time.plusSeconds(4), List.of(customField)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
    // Search using _all field.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "_all:baby",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(1);
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "_all:1234",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(2);
    // Default all field search.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "baby",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(1);
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "1234",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(2);

    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, "baby car 1234", time.plusSeconds(4)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
    // Search using _all field.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "_all:baby",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(2);
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "_all:1234",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(3);
    // Default all field search.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "baby",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(2);
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "1234",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(3);

    // empty string
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(3);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "app*",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(2);

    // Returns baby or car, 2 messages.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "baby car",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(2);

    // Test numbers
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "apple 1234",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(3);

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "123",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(0);
  }

  @Test
  public void testDisabledFullTextSearch() {
    Instant time = Instant.ofEpochSecond(1593365471);
    Trace.KeyValue field1Tag =
        Trace.KeyValue.newBuilder()
            .setVInt32(1234)
            .setKey("field1")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();

    strictLogStoreWithoutFts.logStore.addMessage(
        SpanUtil.makeSpan(1, "apple", time.plusSeconds(4), List.of(field1Tag)));

    strictLogStoreWithoutFts.logStore.addMessage(
        SpanUtil.makeSpan(2, "apple baby", time.plusSeconds(4), List.of(field1Tag)));

    strictLogStoreWithoutFts.logStore.addMessage(
        SpanUtil.makeSpan(3, "baby car 1234", time.plusSeconds(4)));
    strictLogStoreWithoutFts.logStore.commit();
    strictLogStoreWithoutFts.logStore.refresh();

    assertThat(
            strictLogStoreWithoutFts
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "_all:baby",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isZero();

    assertThat(
            strictLogStoreWithoutFts
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "_all:1234",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isZero();

    // Without the _all field as default.
    assertThat(
            strictLogStoreWithoutFts
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "baby",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(2);

    assertThat(
            strictLogStoreWithoutFts
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "1234",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isEqualTo(3);

    // empty string
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isZero();

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "app*",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isZero();

    // Returns baby or car, 2 messages.
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "baby car",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isZero();

    // Test numbers
    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "apple 1234",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isZero();

    assertThat(
            strictLogStore
                .logSearcher
                .search(
                    TEST_DATASET_NAME,
                    "123",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"))
                .hits
                .size())
        .isZero();
  }

  @Test
  public void testNullSearchString() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                strictLogStore.logSearcher.search(
                    TEST_DATASET_NAME + "miss",
                    null,
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s")));
  }

  @Test
  @Disabled // todo - re-enable when multi-tenancy is supported - slackhq/kaldb/issues/223
  public void testMissingIndexSearch() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME + "miss",
            "apple",
            0L,
            MAX_TIME,
            1000,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));

    assertThat(allIndexItems.hits.size()).isEqualTo(0);

    InternalAutoDateHistogram histogram =
        (InternalAutoDateHistogram) Objects.requireNonNull(allIndexItems.internalAggregation);
    assertThat(histogram.getTargetBuckets()).isEqualTo(1);

    assertThat(histogram.getBuckets().size()).isEqualTo(4);
    assertThat(histogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(1).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(2).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(3).getDocCount()).isEqualTo(1);
  }

  @Test
  public void testNoResultQuery() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> elephants =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "elephant",
            0L,
            MAX_TIME,
            1000,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(elephants.hits.size()).isEqualTo(0);

    InternalDateHistogram histogram =
        (InternalDateHistogram) Objects.requireNonNull(elephants.internalAggregation);
    assertThat(histogram.getBuckets().size()).isEqualTo(0);
  }

  @Test
  public void testSearchAndNoStats() {
    Instant time = Instant.now();
    loadTestData(time);
    SearchResult<LogMessage> results =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "_id:Message3 OR _id:Message4",
            time.toEpochMilli(),
            time.plusSeconds(10).toEpochMilli(),
            100,
            null);
    assertThat(results.hits.size()).isEqualTo(2);
    assertThat(results.internalAggregation).isNull();
  }

  @Test
  public void testSearchOnlyHistogram() {
    Instant time = Instant.now();
    loadTestData(time);
    SearchResult<LogMessage> babies =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "_id:Message3 OR _id:Message4",
            time.toEpochMilli(),
            time.plusSeconds(10).toEpochMilli(),
            0,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(babies.hits.size()).isEqualTo(0);

    InternalDateHistogram histogram =
        (InternalDateHistogram) Objects.requireNonNull(babies.internalAggregation);
    assertThat(histogram.getBuckets().size()).isEqualTo(2);

    assertThat(histogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(1).getDocCount()).isEqualTo(1);

    assertThat(
            Long.parseLong(histogram.getBuckets().get(0).getKeyAsString()) >= time.toEpochMilli())
        .isTrue();
    assertThat(
            Long.parseLong(histogram.getBuckets().get(1).getKeyAsString())
                <= time.plusSeconds(10).toEpochMilli())
        .isTrue();
  }

  @Test
  public void testEmptyIndexName() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                strictLogStore.logSearcher.search(
                    "",
                    "test",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s")));
  }

  @Test
  public void testNullIndexName() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                strictLogStore.logSearcher.search(
                    null,
                    "test",
                    0L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s")));
  }

  @Test
  public void testInvalidStartTime() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                strictLogStore.logSearcher.search(
                    TEST_DATASET_NAME,
                    "test",
                    -1L,
                    MAX_TIME,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s")));
  }

  @Test
  public void testInvalidEndTime() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                strictLogStore.logSearcher.search(
                    TEST_DATASET_NAME,
                    "test",
                    0L,
                    -1L,
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s")));
  }

  @Test
  public void testInvalidTimeRange() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                strictLogStore.logSearcher.search(
                    TEST_DATASET_NAME,
                    "test",
                    time.toEpochMilli(),
                    time.minusSeconds(1).toEpochMilli(),
                    1000,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s")));
  }

  @Test
  public void testSearchOrHistogramQuery() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                strictLogStore.logSearcher.search(
                    TEST_DATASET_NAME,
                    "test",
                    time.toEpochMilli(),
                    time.plusSeconds(1).toEpochMilli(),
                    0,
                    null));
  }

  @Test
  public void testNegativeHitCount() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                strictLogStore.logSearcher.search(
                    TEST_DATASET_NAME,
                    "test",
                    time.toEpochMilli(),
                    time.plusSeconds(1).toEpochMilli(),
                    -1,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s")));
  }

  @Test
  public void testNegativeHistogramInterval() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                strictLogStore.logSearcher.search(
                    TEST_DATASET_NAME,
                    "test",
                    time.toEpochMilli(),
                    time.plusSeconds(1).toEpochMilli(),
                    1,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "-1s")));
  }

  @Test
  public void testQueryParseError() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                strictLogStore.logSearcher.search(
                    TEST_DATASET_NAME,
                    "/",
                    time.toEpochMilli(),
                    time.plusSeconds(1).toEpochMilli(),
                    1,
                    new DateHistogramAggBuilder(
                        "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s")));
  }

  @Test
  public void testConcurrentSearches() throws InterruptedException {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    AtomicInteger searchFailures = new AtomicInteger(0);
    AtomicInteger statsFailures = new AtomicInteger(0);
    AtomicInteger searchExceptions = new AtomicInteger(0);
    AtomicInteger successfulRuns = new AtomicInteger(0);

    Runnable searchRun =
        () -> {
          for (int i = 0; i < 100; i++) {
            try {
              SearchResult<LogMessage> babies =
                  strictLogStore.logSearcher.search(
                      TEST_DATASET_NAME,
                      "_id:Message3 OR _id:Message4",
                      0L,
                      MAX_TIME,
                      100,
                      new DateHistogramAggBuilder(
                          "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
              if (babies.hits.size() != 2) {
                searchFailures.addAndGet(1);
              } else {
                successfulRuns.addAndGet(1);
              }
            } catch (Exception e) {
              searchExceptions.addAndGet(1);
            }
          }
        };

    Thread t1 = new Thread(searchRun);
    Thread t2 = new Thread(searchRun);
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    assertThat(searchExceptions.get()).isEqualTo(0);
    assertThat(statsFailures.get()).isEqualTo(0);
    assertThat(searchFailures.get()).isEqualTo(0);
    assertThat(successfulRuns.get()).isEqualTo(200);
  }

  @Test
  public void testSearchById() {
    Instant time = Instant.now();
    loadTestData(time);
    SearchResult<LogMessage> index =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "_id:Message1",
            time.toEpochMilli(),
            time.plusSeconds(2).toEpochMilli(),
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
    assertThat(index.hits.size()).isEqualTo(1);
  }
}
