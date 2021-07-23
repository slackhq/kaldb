package com.slack.kaldb.server;

import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbTimeoutLocalSearcher extends KaldbServiceGrpc.KaldbServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(KaldbTimeoutLocalSearcher.class);

  private final KaldbLocalSearcher kaldbLocalSearcher;
  private final int waitMS;

  public KaldbTimeoutLocalSearcher(KaldbLocalSearcher kaldbLocalSearcher, int waitMS) {
    this.kaldbLocalSearcher = kaldbLocalSearcher;
    this.waitMS = waitMS;
  }

  public void search(
      KaldbSearch.SearchRequest request,
      StreamObserver<KaldbSearch.SearchResult> responseObserver) {
    try {
      Thread.sleep(waitMS);
    } catch (InterruptedException e) {
      LOG.warn("Pause interrupted" + e);
    }
    kaldbLocalSearcher.search(request, responseObserver);
  }
}
