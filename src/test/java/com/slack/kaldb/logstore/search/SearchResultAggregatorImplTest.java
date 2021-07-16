package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.testlib.HistogramUtil.makeHistogram;
import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.internal.shaded.futures.CompletableFutures;
import com.slack.kaldb.histogram.Histogram;
import com.slack.kaldb.histogram.HistogramBucket;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.testlib.MessageUtil;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.junit.Test;

public class SearchResultAggregatorImplTest {
  private SearchResult<LogMessage> makeSearchResult(
      List<LogMessage> messages,
      long tookMs,
      long totalCount,
      List<HistogramBucket> buckets,
      int failedNodes,
      int totalNodes,
      int totalSnapshots,
      int snapshotsWithReplicas) {
    return new SearchResult<>(
        messages,
        tookMs,
        totalCount,
        buckets,
        failedNodes,
        totalNodes,
        totalSnapshots,
        snapshotsWithReplicas);
  }

  @Test
  public void testSimpleSearchResultsAggWithOneResult() {
    long tookMs = 10;
    int bucketCount = 12;
    int howMany = 1;
    Instant startTime1 = LocalDateTime.of(2020, 1, 1, 1, 0, 0).atZone(ZoneOffset.UTC).toInstant();
    Instant startTime2 = LocalDateTime.of(2020, 1, 1, 2, 0, 0).atZone(ZoneOffset.UTC).toInstant();
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);

    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    Histogram histogram1 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages1);
    Histogram histogram2 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages2);

    SearchResult<LogMessage> searchResult1 =
        makeSearchResult(messages1, tookMs, 10, histogram1.getBuckets(), 0, 1, 1, 0);
    SearchResult<LogMessage> searchResult2 =
        makeSearchResult(messages2, tookMs + 1, 10, histogram2.getBuckets(), 0, 1, 1, 0);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_INDEX_NAME,
            "Message1",
            histogramStartMs,
            histogramEndMs,
            howMany,
            bucketCount);
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(makeAsyncResults(searchResults))
            .join();

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(0);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(2);

    LogMessage hit = aggSearchResult.hits.get(0);
    assertThat(hit.id).contains("Message20");
    assertThat(hit.timeSinceEpochMilli)
        .isEqualTo(startTime2.plus(9, ChronoUnit.MINUTES).toEpochMilli());

    assertThat(aggSearchResult.totalCount).isEqualTo(20);
    for (HistogramBucket b : aggSearchResult.buckets) {
      assertThat(b.getCount() == 10 || b.getCount() == 0).isTrue();
    }
  }

  @Test
  public void testSimpleSearchResultsAggWithMultipleResults() {
    long tookMs = 10;
    int bucketCount = 12;
    int howMany = 10;
    Instant startTime1 = LocalDateTime.of(2020, 1, 1, 1, 0, 0).atZone(ZoneOffset.UTC).toInstant();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    Histogram histogram1 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages1);
    Histogram histogram2 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages2);

    SearchResult<LogMessage> searchResult1 =
        makeSearchResult(messages1, tookMs, 10, histogram1.getBuckets(), 0, 1, 1, 0);
    SearchResult<LogMessage> searchResult2 =
        makeSearchResult(messages2, tookMs + 1, 10, histogram2.getBuckets(), 0, 1, 1, 0);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_INDEX_NAME,
            "Message1",
            histogramStartMs,
            histogramEndMs,
            howMany,
            bucketCount);
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(makeAsyncResults(searchResults))
            .join();

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(0);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(2);

    for (LogMessage m : aggSearchResult.hits) {
      assertThat(messages2.contains(m)).isTrue();
    }

    assertThat(aggSearchResult.totalCount).isEqualTo(20);
    for (HistogramBucket b : aggSearchResult.buckets) {
      assertThat(b.getCount() == 10 || b.getCount() == 0).isTrue();
    }
  }

  @Test
  public void testSearchResultAggregatorOn4Results() {
    long tookMs = 10;
    int bucketCount = 24;
    int howMany = 10;
    Instant startTime1 = LocalDateTime.of(2020, 1, 1, 1, 0, 0).atZone(ZoneOffset.UTC).toInstant();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    Instant startTime3 = startTime1.plus(2, ChronoUnit.HOURS);
    Instant startTime4 = startTime1.plus(3, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(4, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);
    List<LogMessage> messages3 =
        MessageUtil.makeMessagesWithTimeDifference(21, 30, 1000 * 60, startTime3);
    List<LogMessage> messages4 =
        MessageUtil.makeMessagesWithTimeDifference(31, 40, 1000 * 60, startTime4);

    Histogram histogram1 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages1);
    Histogram histogram2 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages2);
    Histogram histogram3 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages3);
    Histogram histogram4 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages4);

    SearchResult<LogMessage> searchResult1 =
        makeSearchResult(messages1, tookMs, 10, histogram1.getBuckets(), 0, 1, 1, 0);
    SearchResult<LogMessage> searchResult2 =
        makeSearchResult(messages2, tookMs + 1, 10, histogram2.getBuckets(), 1, 1, 1, 1);
    SearchResult<LogMessage> searchResult3 =
        makeSearchResult(messages3, tookMs + 2, 10, histogram3.getBuckets(), 0, 1, 1, 0);
    SearchResult<LogMessage> searchResult4 =
        makeSearchResult(messages4, tookMs + 3, 10, histogram4.getBuckets(), 0, 1, 1, 1);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_INDEX_NAME,
            "Message1",
            histogramStartMs,
            histogramEndMs,
            howMany,
            bucketCount);
    List<SearchResult<LogMessage>> searchResults =
        List.of(searchResult1, searchResult4, searchResult3, searchResult2);
    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(makeAsyncResults(searchResults))
            .join();

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 3);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(1);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(2);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(4);

    for (LogMessage m : aggSearchResult.hits) {
      assertThat(messages4.contains(m)).isTrue();
    }

    assertThat(aggSearchResult.totalCount).isEqualTo(40);
    for (HistogramBucket b : aggSearchResult.buckets) {
      assertThat(b.getCount() == 10 || b.getCount() == 0).isTrue();
    }
  }

  @Test
  public void testSimpleSearchResultsAggWithNoHistograms() {
    long tookMs = 10;
    int bucketCount = 0;
    int howMany = 10;
    Instant startTime1 = LocalDateTime.of(2020, 1, 1, 1, 0, 0).atZone(ZoneOffset.UTC).toInstant();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long searchStartMs = startTime1.toEpochMilli();
    long searchEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    SearchResult<LogMessage> searchResult1 =
        makeSearchResult(messages1, tookMs, 10, Collections.emptyList(), 0, 1, 1, 0);
    SearchResult<LogMessage> searchResult2 =
        makeSearchResult(messages2, tookMs + 1, 10, Collections.emptyList(), 0, 1, 1, 0);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_INDEX_NAME,
            "Message1",
            searchStartMs,
            searchEndMs,
            howMany,
            bucketCount);
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(makeAsyncResults(searchResults))
            .join();

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(0);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(2);

    for (LogMessage m : aggSearchResult.hits) {
      assertThat(messages2.contains(m)).isTrue();
    }

    assertThat(aggSearchResult.totalCount).isEqualTo(20);
    assertThat(aggSearchResult.buckets.size()).isZero();
  }

  @Test
  public void testSimpleSearchResultsAggNoHits() {
    long tookMs = 10;
    int bucketCount = 12;
    int howMany = 0;
    Instant startTime1 = LocalDateTime.of(2020, 1, 1, 1, 0, 0).atZone(ZoneOffset.UTC).toInstant();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    Histogram histogram1 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages1);
    Histogram histogram2 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages2);

    SearchResult<LogMessage> searchResult1 =
        makeSearchResult(Collections.emptyList(), tookMs, 7, histogram1.getBuckets(), 0, 2, 2, 2);
    SearchResult<LogMessage> searchResult2 =
        makeSearchResult(
            Collections.emptyList(), tookMs + 1, 8, histogram2.getBuckets(), 0, 1, 1, 0);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_INDEX_NAME,
            "Message1",
            histogramStartMs,
            histogramEndMs,
            howMany,
            bucketCount);
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(makeAsyncResults(searchResults))
            .join();

    assertThat(aggSearchResult.hits.size()).isZero();
    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(2);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(3);
    assertThat(aggSearchResult.totalCount).isEqualTo(15);
    for (HistogramBucket b : aggSearchResult.buckets) {
      assertThat(b.getCount() == 10 || b.getCount() == 0).isTrue();
    }
  }

  @Test
  public void testSearchResultsAggIgnoresBucketsInSearchResultsSafely() {
    long tookMs = 10;
    int bucketCount = 0;
    int howMany = 10;
    Instant startTime1 = LocalDateTime.of(2020, 1, 1, 1, 0, 0).atZone(ZoneOffset.UTC).toInstant();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long startTimeMs = startTime1.toEpochMilli();
    long endTimeMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    Histogram histogram1 = makeHistogram(startTimeMs, endTimeMs, 2, messages1);

    SearchResult<LogMessage> searchResult1 =
        makeSearchResult(messages1, tookMs, 10, histogram1.getBuckets(), 1, 1, 1, 0);
    SearchResult<LogMessage> searchResult2 =
        makeSearchResult(messages2, tookMs + 1, 11, Collections.emptyList(), 0, 1, 1, 0);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_INDEX_NAME, "Message1", startTimeMs, endTimeMs, howMany, bucketCount);
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(makeAsyncResults(searchResults))
            .join();

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(1);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(0);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(2);

    for (LogMessage m : aggSearchResult.hits) {
      assertThat(messages2.contains(m)).isTrue();
    }

    assertThat(aggSearchResult.totalCount).isEqualTo(21);
    assertThat(aggSearchResult.buckets.size()).isZero();
  }

  @Test
  public void testSimpleSearchResultsAggIgnoreHitsSafely() {
    long tookMs = 10;
    int bucketCount = 12;
    int howMany = 0;
    Instant startTime1 = LocalDateTime.of(2020, 1, 1, 1, 0, 0).atZone(ZoneOffset.UTC).toInstant();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    Histogram histogram1 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages1);
    Histogram histogram2 = makeHistogram(histogramStartMs, histogramEndMs, bucketCount, messages2);

    SearchResult<LogMessage> searchResult1 =
        makeSearchResult(messages1, tookMs, 7, histogram1.getBuckets(), 0, 2, 2, 2);
    SearchResult<LogMessage> searchResult2 =
        makeSearchResult(
            Collections.emptyList(), tookMs + 1, 8, histogram2.getBuckets(), 0, 1, 1, 0);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_INDEX_NAME,
            "Message1",
            histogramStartMs,
            histogramEndMs,
            howMany,
            bucketCount);
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(makeAsyncResults(searchResults))
            .join();

    assertThat(aggSearchResult.hits.size()).isZero();
    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(2);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(3);
    assertThat(aggSearchResult.totalCount).isEqualTo(15);
    for (HistogramBucket b : aggSearchResult.buckets) {
      assertThat(b.getCount() == 10 || b.getCount() == 0).isTrue();
    }
  }

  public CompletableFuture<List<SearchResult<LogMessage>>> makeAsyncResults(
      List<SearchResult<LogMessage>> searchResults) {
    return CompletableFutures.allAsList(
        searchResults
            .stream()
            .map(result -> CompletableFuture.supplyAsync(() -> result))
            .collect(Collectors.toList()));
  }
}
