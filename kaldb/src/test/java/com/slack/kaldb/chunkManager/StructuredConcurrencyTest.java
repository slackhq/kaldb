package com.slack.kaldb.chunkManager;

import java.time.Instant;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class StructuredConcurrencyTest {
  public static void main(String[] args) {
    Test test = new Test();
    test.query();
  }
}

class Test {
  public void query() {
    try (CollectingScope<String> scope = new CollectingScope<>()) {
      scope.fork(() -> getQuery("query1", -1));
      scope.fork(() -> getQuery("query2", 50));
      scope.fork(() -> getQuery("query3", -1));
      scope.fork(() -> getQuery("query4", -1));

      try {
        scope.joinUntil(Instant.now().plusMillis(10));
      } catch (TimeoutException e) {
        System.out.println("Some queries took to long so we cancelled it");
      }

      // The ordering is based on whichever task completes first
      String results = scope.completedSuccessfully()
              .collect(Collectors.joining(", ", "{ ", " }"));
      System.out.println(results);
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  public String getQuery(String query, long waitForMs) {
    if (waitForMs > 0) {
      try {
        Thread.sleep(waitForMs);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    if ("query3".equals(query)) {
      throw new IllegalArgumentException("Wrong query parameters");
    }
    return "result_" + query;
  }
}

class CollectingScope<T> extends StructuredTaskScope<T> {
  private final Queue<T> subtasks = new ConcurrentLinkedQueue<>();

  @Override
  protected void handleComplete(Subtask<? extends T> subtask) {
    if (subtask.state() == Subtask.State.SUCCESS) {
      subtasks.add(subtask.get());
    }
  }

  @Override
  public CollectingScope<T> join() throws InterruptedException {
    super.join();
    return this;
  }

  public Stream<T> completedSuccessfully() {
    // If we call ensureOwnerAndJoined we get a IllegalStateException
    // The reason being on timeout lastJoinCompleted does not get updated by design
    // But I can't figure out a way to get the successful results otherwise
    // super.ensureOwnerAndJoined();
    return subtasks.stream();
  }
}
