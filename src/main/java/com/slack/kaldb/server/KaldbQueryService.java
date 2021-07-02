package com.slack.kaldb.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.internal.shaded.futures.CompletableFutures;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.logstore.search.SearchResultAggregatorImpl;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.spotify.futures.ListenableFuturesExtra;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbQueryService extends KaldbServiceGrpc.KaldbServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(KaldbQueryService.class);

  public static List<String> servers = new ArrayList<>();

  // public so that we can override in tests
  public static int READ_TIMEOUT_MS = 15000;

  // TODO Cache the stub
  // TODO Integrate with ZK to update list of servers

  public List<KaldbSearch.SearchResult> distributedSearch(KaldbSearch.SearchRequest request) {

    List<KaldbSearch.SearchResult> searchResults = new ArrayList<>();
    List<CompletableFuture<KaldbSearch.SearchResult>> futures = new ArrayList<>(servers.size());

    for (String server : servers) {
      KaldbServiceGrpc.KaldbServiceFutureStub stub =
          Clients.newClient(server, KaldbServiceGrpc.KaldbServiceFutureStub.class)
              .withDeadlineAfter(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      CompletableFuture<KaldbSearch.SearchResult> completable =
          ListenableFuturesExtra.toCompletableFuture(stub.search(request));
      futures.add(completable);
    }

    CompletableFuture<List<KaldbSearch.SearchResult>> allFutures =
        CompletableFutures.successfulAsList(
            futures, t -> KaldbSearch.SearchResult.newBuilder().build());

    try {
      searchResults = allFutures.join();
    } catch (Exception e) {
      LOG.warn("Could not finish futures get within read timeout");
    }
    return searchResults;
  }

  @Override
  public void search(
      KaldbSearch.SearchRequest request,
      StreamObserver<KaldbSearch.SearchResult> responseObserver) {

    List<KaldbSearch.SearchResult> protoSearchResults = distributedSearch(request);

    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(protoSearchResults.size());
    for (KaldbSearch.SearchResult protoSearchResult : protoSearchResults) {
      try {
        searchResults.add(KaldbLocalSearcher.fromSearchResultProto(protoSearchResult));
      } catch (IOException e) {
        LOG.warn(
            "Unable to parse proto search result to search result for object " + protoSearchResult);
      }
    }

    SearchResult<LogMessage> searchResult =
        new SearchResultAggregatorImpl<>()
            .aggregate(searchResults, KaldbLocalSearcher.fromSearchRequest(request));

    try {
      KaldbSearch.SearchResult aggregatedProtoResult =
          KaldbLocalSearcher.toSearchResultProto(searchResult);
      responseObserver.onNext(aggregatedProtoResult);
      responseObserver.onCompleted();
    } catch (JsonProcessingException e) {
      LOG.warn("Unable to convert aggregated search result to proto search result");
    }
  }
}
