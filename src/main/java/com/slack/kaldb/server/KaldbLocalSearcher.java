package com.slack.kaldb.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import com.slack.kaldb.chunk.ChunkManager;
import com.slack.kaldb.histogram.HistogramBucket;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.util.JsonUtil;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbLocalSearcher<T> extends KaldbServiceGrpc.KaldbServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbLocalSearcher.class);

  private final ChunkManager<T> chunkManager;

  public KaldbLocalSearcher(ChunkManager<T> chunkManager) {
    this.chunkManager = chunkManager;
  }

  public static SearchQuery fromSearchRequest(KaldbSearch.SearchRequest searchRequest) {
    return new SearchQuery(
        searchRequest.getIndexName(),
        searchRequest.getQueryString(),
        searchRequest.getStartTimeEpochMs(),
        searchRequest.getEndTimeEpochMs(),
        searchRequest.getHowMany(),
        searchRequest.getBucketCount());
  }

  public static SearchResult<LogMessage> fromSearchResultProto(
      KaldbSearch.SearchResult protoSearchResult) {
    try {
      List<LogMessage> hits = new ArrayList<>(protoSearchResult.getHitsCount());

      for (ByteString bytes : protoSearchResult.getHitsList().asByteStringList()) {
        LogWireMessage hit = JsonUtil.read(bytes.toStringUtf8(), LogWireMessage.class);
        LogMessage message = LogMessage.fromWireMessage(hit);
        hits.add(message);
      }
      List<HistogramBucket> histogramBuckets = new ArrayList<>();
      for (KaldbSearch.HistogramBucket protoBucket : protoSearchResult.getBucketsList()) {
        histogramBuckets.add(new HistogramBucket(protoBucket.getLow(), protoBucket.getHigh()));
      }

      return new SearchResult<>(
          hits,
          protoSearchResult.getTookMicros(),
          protoSearchResult.getTotalCount(),
          histogramBuckets,
          protoSearchResult.getFailedNodes(),
          protoSearchResult.getTotalNodes(),
          protoSearchResult.getTotalSnapshots(),
          protoSearchResult.getSnapshotsWithReplicas());
    } catch (IOException e) {
      return new SearchResult<>(null, 0, 0, null, 1, 1, 0, 0);
    }
  }

  public static <T> CompletableFuture<KaldbSearch.SearchResult> toSearchResultProto(
      CompletableFuture<SearchResult<T>> searchResult) {
    return searchResult.thenApply(KaldbLocalSearcher::toSearchResultProto);
  }

  private static <T> KaldbSearch.SearchResult toSearchResultProto(SearchResult<T> searchResult) {
    KaldbSearch.SearchResult.Builder searchResultBuilder = KaldbSearch.SearchResult.newBuilder();
    searchResultBuilder.setTotalCount(searchResult.totalCount);
    searchResultBuilder.setTookMicros(searchResult.tookMicros);
    searchResultBuilder.setFailedNodes(searchResult.failedNodes);
    searchResultBuilder.setTotalNodes(searchResult.totalNodes);
    searchResultBuilder.setTotalSnapshots(searchResult.totalSnapshots);
    searchResultBuilder.setSnapshotsWithReplicas(searchResult.snapshotsWithReplicas);

    // Set hits
    ArrayList<String> protoHits = new ArrayList<>(searchResult.hits.size());
    for (T hit : searchResult.hits) {
      try {
        protoHits.add(JsonUtil.writeAsString(hit));
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }
    }
    searchResultBuilder.addAllHits(protoHits);

    // Set buckets
    List<KaldbSearch.HistogramBucket> protoBuckets = new ArrayList<>(searchResult.buckets.size());
    for (HistogramBucket bucket : searchResult.buckets) {
      KaldbSearch.HistogramBucket.Builder builder = KaldbSearch.HistogramBucket.newBuilder();
      protoBuckets.add(
          builder
              .setCount(bucket.getCount())
              .setHigh(bucket.getHigh())
              .setLow(bucket.getLow())
              .build());
    }
    searchResultBuilder.addAllBuckets(protoBuckets);

    return searchResultBuilder.build();
  }

  public CompletableFuture<KaldbSearch.SearchResult> doSearch(KaldbSearch.SearchRequest request) {
    SearchQuery query = fromSearchRequest(request);
    CompletableFuture<SearchResult<T>> searchResult = chunkManager.query(query);
    return toSearchResultProto(searchResult);
  }

  @Override
  public void search(
      KaldbSearch.SearchRequest request,
      StreamObserver<KaldbSearch.SearchResult> responseObserver) {

    CompletableFuture<KaldbSearch.SearchResult> protoSearchResult = doSearch(request);
    protoSearchResult.whenCompleteAsync(
        (result, t) -> {
          if (t != null) {
            LOG.error("Error completing the future", t);
            responseObserver.onError(t);
          } else {
            responseObserver.onNext(result);
            responseObserver.onCompleted();
          }
        });
  }
}
