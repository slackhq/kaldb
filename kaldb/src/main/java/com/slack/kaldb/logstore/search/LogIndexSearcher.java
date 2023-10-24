package com.slack.kaldb.logstore.search;

import com.slack.kaldb.logstore.search.aggregations.AggBuilder;
import java.io.Closeable;

public interface LogIndexSearcher<T> extends Closeable {
  SearchResult<T> search(
      String dataset, String query, Long minTime, Long maxTime, int howMany, AggBuilder aggBuilder);
}
