package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.chunk.ChunkInfo.toSnapshotMetadata;
import static com.slack.kaldb.chunk.ReadWriteChunk.LIVE_SNAPSHOT_PREFIX;
import static com.slack.kaldb.chunk.ReadWriteChunk.toSearchMetadata;
import static com.slack.kaldb.logstore.search.KaldbDistributedQueryService.getMatchingSearchMetadata;
import static com.slack.kaldb.logstore.search.KaldbDistributedQueryService.getMatchingSnapshots;
import static com.slack.kaldb.logstore.search.KaldbDistributedQueryService.getNodesAndSnapshotsToQuery;
import static com.slack.kaldb.metadata.snapshot.SnapshotMetadata.LIVE_SNAPSHOT_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import brave.Tracing;
import com.google.common.util.concurrent.Futures;
import com.slack.kaldb.chunk.ChunkInfo;
import com.slack.kaldb.chunk.ReadOnlyChunkImpl;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.metadata.core.KaldbMetadataTestUtils;
import com.slack.kaldb.metadata.dataset.DatasetMetadata;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata;
import com.slack.kaldb.metadata.search.SearchMetadata;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.proto.schema.Schema;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KaldbDistributedQueryServiceTest {

  private SimpleMeterRegistry metricsRegistry;

  private AsyncCuratorFramework curatorFramework;
  private SearchMetadataStore searchMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private DatasetMetadataStore datasetMetadataStore;

  private TestingServer testZKServer;
  private SearchContext indexer1SearchContext;
  private SearchContext indexer2SearchContext;
  private SearchContext cache1SearchContext;
  private SearchContext cache2SearchContext;
  private SearchContext cache3SearchContext;
  private SearchContext cache4SearchContext;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();

    metricsRegistry = new SimpleMeterRegistry();
    testZKServer = new TestingServer();

    // Metadata store
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testZKServer.getConnectString())
            .setZkPathPrefix("indexerTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    curatorFramework = spy(CuratorBuilder.build(metricsRegistry, zkConfig));

    snapshotMetadataStore = spy(new SnapshotMetadataStore(curatorFramework, metricsRegistry));
    searchMetadataStore = spy(new SearchMetadataStore(curatorFramework, true, metricsRegistry));
    datasetMetadataStore = new DatasetMetadataStore(curatorFramework, true, metricsRegistry);

    indexer1SearchContext = new SearchContext("indexer_host1", 10000);
    indexer2SearchContext = new SearchContext("indexer_host2", 10001);
    cache1SearchContext = new SearchContext("cache_host1", 20000);
    cache2SearchContext = new SearchContext("cache_host2", 20001);
    cache3SearchContext = new SearchContext("cache_host3", 20002);
    cache4SearchContext = new SearchContext("cache_host4", 20003);
  }

  @AfterEach
  public void tearDown() throws Exception {
    snapshotMetadataStore.close();
    searchMetadataStore.close();
    datasetMetadataStore.close();
    curatorFramework.unwrap().close();
    metricsRegistry.close();
    testZKServer.close();
  }

  @Test
  public void testOneIndexer() {
    String indexName = "testIndex";
    Instant chunk1CreationTime = Instant.ofEpochMilli(100);
    Instant chunk1EndTime = Instant.ofEpochMilli(200);
    String chunk1Name =
        createIndexerZKMetadata(chunk1CreationTime, chunk1EndTime, "1", indexer1SearchContext);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 2);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 1);

    // we don't have any dataset metadata entry, so we shouldn't be able to find any snapshot
    Map<String, List<String>> searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(0);

    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 300, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    // now we can find the snapshot
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer1SearchContext.toString());
    List<String> chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    assertThat(chunks.iterator().next()).isEqualTo(chunk1Name);

    // we can't find snapshot since the time window doesn't match snapshot
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1EndTime.toEpochMilli() + 1,
            chunk1EndTime.toEpochMilli() + 100,
            indexName);
    assertThat(searchNodes.size()).isEqualTo(0);

    // add another chunk on the same indexer and ensure we still find the node
    Instant chunk2CreationTime = Instant.ofEpochMilli(201);
    Instant chunk2EndTime = Instant.ofEpochMilli(300);
    String chunk2Name =
        createIndexerZKMetadata(chunk2CreationTime, chunk2EndTime, "1", indexer1SearchContext);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 4);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 2);
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore, searchMetadataStore, datasetMetadataStore, 0, 300, indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer1SearchContext.toString());

    // if we request a time window that matches both chunks the searchable chunks will be 2
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore, searchMetadataStore, datasetMetadataStore, 0, 250, indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer1SearchContext.toString());
    chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(2);
    Iterator<String> chunkIter = chunks.iterator();
    String result1 = chunkIter.next();
    String result2 = chunkIter.next();
    assertThat(result1.equals(chunk1Name) || result1.equals(chunk2Name)).isTrue();
    assertThat(result2.equals(chunk1Name) || result2.equals(chunk2Name)).isTrue();

    // request a time window that matches only 1 chunk
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore, searchMetadataStore, datasetMetadataStore, 250, 300, indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer1SearchContext.toString());
    chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(chunk2Name);

    // re-add dataset metadata with a different time window that doesn't match any snapshot
    datasetMetadataStore.deleteAsync(datasetMetadata.name);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 0);
    partition = new DatasetPartitionMetadata(1, 99, List.of("1"));
    datasetMetadata = new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    // we can't find snapshot since the time window doesn't match any dataset metadata
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(0);
  }

  @Test
  // snaphost1[100-200] -> hosted on indexer1 , cache1 , cache2
  // snapshot2[51-150] -> cache2
  // snapshot3[151-250] - cache1
  public void testOneIndexerTwoCacheNode()
      throws ExecutionException, InterruptedException, TimeoutException {
    String indexName = "testIndex";
    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 500, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    Instant chunk1CreationTime = Instant.ofEpochMilli(100);
    Instant chunk1EndTime = Instant.ofEpochMilli(200);
    String snapshot1Name =
        createIndexerZKMetadata(chunk1CreationTime, chunk1EndTime, "1", indexer1SearchContext);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isEqualTo(2);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size()).isEqualTo(1);

    AtomicReference<Map<String, List<String>>> searchNodes = new AtomicReference<>();
    await()
        .until(
            () -> {
              searchNodes.set(
                  getSearchNodesToQuery(
                      snapshotMetadataStore,
                      searchMetadataStore,
                      datasetMetadataStore,
                      chunk1CreationTime.toEpochMilli(),
                      chunk1EndTime.toEpochMilli(),
                      indexName));
              return searchNodes.get().size() == 1;
            });

    assertThat(searchNodes.get().size()).isEqualTo(1);
    assertThat(searchNodes.get().keySet().iterator().next())
        .isEqualTo(indexer1SearchContext.toString());
    List<String> chunks = searchNodes.get().values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    Iterator<String> chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(snapshot1Name);

    // create cache node entry for search metadata also serving the snapshot
    SearchMetadata cacheNodeSearchMetada =
        ReadOnlyChunkImpl.registerSearchMetadata(
            searchMetadataStore, cache1SearchContext, snapshot1Name);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 2);

    searchNodes.set(
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName));
    assertThat(searchNodes.get().size()).isEqualTo(1);
    assertThat(searchNodes.get().keySet().iterator().next())
        .isEqualTo(cache1SearchContext.toString());
    chunks = searchNodes.get().values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(cacheNodeSearchMetada.snapshotName);

    // create cache node entry for search metadata also serving the snapshot
    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache2SearchContext, snapshot1Name);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 3);

    searchNodes.set(
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName));
    assertThat(searchNodes.get().size()).isEqualTo(1);
    String searchNodeUrl = searchNodes.get().keySet().iterator().next();
    assertThat(
            searchNodeUrl.equals(cache1SearchContext.toString())
                || searchNodeUrl.equals(cache2SearchContext.toString())
                || searchNodeUrl.equals(cache3SearchContext.toString()))
        .isTrue();
    chunks = searchNodes.get().values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(cacheNodeSearchMetada.snapshotName);

    Instant snapshot2CreationTime = Instant.ofEpochMilli(51);
    Instant snapshot2EndTime = Instant.ofEpochMilli(150);
    SnapshotMetadata snapshot2Metadata =
        createSnapshot(snapshot2CreationTime, snapshot2EndTime, false, "1");
    await()
        .until(
            () ->
                KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()
                    == 3); // snapshot1(live + non_live) + snapshot2

    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache2SearchContext, snapshot2Metadata.name);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 4);

    Instant snapshot3CreationTime = Instant.ofEpochMilli(151);
    Instant snapshot3EndTime = Instant.ofEpochMilli(250);
    SnapshotMetadata snapshot3Metadata =
        createSnapshot(snapshot3CreationTime, snapshot3EndTime, false, "1");
    await()
        .until(
            () ->
                KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()
                    == 4); // snapshot1(live + non_live) + snapshot2 + snapshot3

    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache1SearchContext, snapshot3Metadata.name);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 5);

    searchNodes.set(
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName));
    assertThat(searchNodes.get().size()).isEqualTo(2);
    // snapshot1 ( could be picked from either cache1 or cache2) + snapshot2 + snapshot3
    int totalChunksToBeSearched = 3;
    int count = 0;
    for (Map.Entry<String, List<String>> node : searchNodes.get().entrySet()) {
      // must not be the index node
      assertThat(searchNodeUrl.equals(indexer1SearchContext.toString())).isFalse();
      count += node.getValue().size();
    }
    assertThat(count).isEqualTo(totalChunksToBeSearched);

    searchNodes.set(
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            snapshot2CreationTime.toEpochMilli(),
            snapshot2EndTime.toEpochMilli(),
            indexName));
    assertThat(searchNodes.get().size() == 1 || searchNodes.get().size() == 2).isTrue();
    if (searchNodes.get().size() == 1) {
      chunks = searchNodes.get().values().iterator().next();
      assertThat(chunks.size()).isEqualTo(2);
    } else {
      for (Map.Entry<String, List<String>> searchNode : searchNodes.get().entrySet()) {
        if (searchNode.getKey().equals(cache1SearchContext.toUrl())) {
          assertThat(searchNode.getValue().size()).isEqualTo(1);
          assertThat(searchNode.getValue().iterator().next()).isEqualTo(snapshot1Name);
        } else if (searchNode.getKey().equals(cache2SearchContext.toUrl())) {
          assertThat(searchNode.getValue().size()).isEqualTo(1);
          assertThat(searchNode.getValue().iterator().next()).isEqualTo(snapshot2Metadata.name);
        } else {
          fail(
              "SearchNodes should only query cache1 and cache2 but is trying to search "
                  + searchNode.getKey());
        }
      }
    }
  }

  @Test
  public void testOneIndexerOneCache() throws Exception {
    String indexName = "testIndex";
    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(199, 500, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    Instant chunk1CreationTime = Instant.ofEpochMilli(100);
    Instant chunk1EndTime = Instant.ofEpochMilli(200);
    String snapshot1Name =
        createIndexerZKMetadata(chunk1CreationTime, chunk1EndTime, "1", indexer1SearchContext);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isEqualTo(2);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size()).isEqualTo(1);

    await().until(() -> snapshotMetadataStore.listSync().size(), (size) -> size == 2);
    await().until(() -> searchMetadataStore.listSync().size(), (size) -> size == 1);

    Map<String, List<String>> searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer1SearchContext.toString());
    List<String> chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    Iterator<String> chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(snapshot1Name);

    Instant chunk2CreationTime = Instant.ofEpochMilli(201);
    Instant chunk2EndTime = Instant.ofEpochMilli(300);
    String snapshot2Name =
        createIndexerZKMetadata(chunk2CreationTime, chunk2EndTime, "1", indexer1SearchContext);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()).isEqualTo(4);
    assertThat(KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size()).isEqualTo(2);

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer1SearchContext.toString());
    chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(snapshot1Name);

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk2CreationTime.toEpochMilli(),
            chunk2EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer1SearchContext.toString());
    chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(snapshot2Name);

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk2EndTime.toEpochMilli() + 1,
            chunk2EndTime.toEpochMilli() + 100,
            indexName);
    assertThat(searchNodes.size()).isEqualTo(0);

    // create cache node entry for search metadata also serving the snapshot
    SearchMetadata cacheNodeSearchMetada =
        ReadOnlyChunkImpl.registerSearchMetadata(
            searchMetadataStore, cache1SearchContext, snapshot1Name);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 3);

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(cache1SearchContext.toString());
    chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(cacheNodeSearchMetada.snapshotName);

    // re-add dataset metadata with a different time window that doesn't match any snapshot
    datasetMetadataStore.deleteAsync(datasetMetadata.name);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 0);
    partition = new DatasetPartitionMetadata(1, 99, List.of("1"));
    datasetMetadata = new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    // we can't find snapshot since the time window doesn't match any dataset metadata
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunk1CreationTime.toEpochMilli(),
            chunk1EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(0);
  }

  @Test
  // snaphost1[100-200] -> hosted on cache1 , cache2
  // snapshot2[51-150] -> cache2
  // snapshot3[151-250] - cache1
  public void testTwoCacheNodes() throws Exception {
    String indexName = "testIndex";
    // create snapshot
    Instant chunkCreationTime = Instant.ofEpochMilli(100);
    Instant chunkEndTime = Instant.ofEpochMilli(200);
    SnapshotMetadata snapshotMetadata = createSnapshot(chunkCreationTime, chunkEndTime, false, "1");
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 1);

    // create first search metadata hosted by cache1
    SearchMetadata cache1NodeSearchMetada =
        ReadOnlyChunkImpl.registerSearchMetadata(
            searchMetadataStore, cache1SearchContext, snapshotMetadata.name);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 1);

    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 500, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName, "testOwner", 1, List.of(partition), indexName);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    // assert search will always find cache1
    Map<String, List<String>> searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            1,
            chunkCreationTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(cache1SearchContext.toString());
    List<String> chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    Iterator<String> chunkIter = chunks.iterator();
    assertThat(chunkIter.next()).isEqualTo(cache1NodeSearchMetada.snapshotName);

    // create second search metadata hosted by cache2
    SearchMetadata cache2NodeSearchMetada =
        ReadOnlyChunkImpl.registerSearchMetadata(
            searchMetadataStore, cache2SearchContext, snapshotMetadata.name);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 2);

    // assert search will always find cache1 or cache2
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            0,
            chunkCreationTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(1);
    String searchNodeUrl = searchNodes.keySet().iterator().next();
    assertThat(
            searchNodeUrl.equals(cache1SearchContext.toString())
                || searchNodeUrl.equals(cache2SearchContext.toString()))
        .isTrue();
    chunks = searchNodes.values().iterator().next();
    assertThat(chunks.size()).isEqualTo(1);
    chunkIter = chunks.iterator();
    String chunkName = chunkIter.next();
    assertThat(
            chunkName.equals(cache1NodeSearchMetada.snapshotName)
                || chunkName.equals(cache2NodeSearchMetada.snapshotName))
        .isTrue();

    // now add snapshot2 to cache2
    Instant snapshot2CreationTime = Instant.ofEpochMilli(51);
    Instant snapshot2EndTime = Instant.ofEpochMilli(150);
    SnapshotMetadata snapshot2Metadata =
        createSnapshot(snapshot2CreationTime, snapshot2EndTime, false, "1");
    await()
        .until(
            () ->
                KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()
                    == 2); // snapshot1 + snapshot2

    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache2SearchContext, snapshot2Metadata.name);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 3);

    // now add snapshot3 to cache1
    Instant snapshot3CreationTime = Instant.ofEpochMilli(151);
    Instant snapshot3EndTime = Instant.ofEpochMilli(250);
    SnapshotMetadata snapshot3Metadata =
        createSnapshot(snapshot3CreationTime, snapshot3EndTime, false, "1");
    await()
        .until(
            () ->
                KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size()
                    == 3); // snapshot1 + snapshot2 + snapshot3

    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache1SearchContext, snapshot3Metadata.name);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 4);

    // assert search will always find cache1 AND cache2
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size()).isEqualTo(2);
    // snapshot1 ( could be picked from either cache1 or cache2) + snapshot2 + snapshot3
    int totalChunksToBeSearched = 3;
    int count = 0;
    for (Map.Entry<String, List<String>> node : searchNodes.entrySet()) {
      count += node.getValue().size();
    }
    assertThat(count).isEqualTo(totalChunksToBeSearched);

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            snapshot2CreationTime.toEpochMilli(),
            snapshot2EndTime.toEpochMilli(),
            indexName);
    assertThat(searchNodes.size() == 1 || searchNodes.size() == 2).isTrue();
    if (searchNodes.size() == 1) {
      chunks = searchNodes.values().iterator().next();
      assertThat(chunks.size()).isEqualTo(2);
    } else {
      for (Map.Entry<String, List<String>> searchNode : searchNodes.entrySet()) {
        if (searchNode.getKey().equals(cache1SearchContext.toUrl())) {
          assertThat(searchNode.getValue().size()).isEqualTo(1);
          assertThat(searchNode.getValue().iterator().next()).isEqualTo(snapshotMetadata.name);
        } else if (searchNode.getKey().equals(cache2SearchContext.toUrl())) {
          assertThat(searchNode.getValue().size()).isEqualTo(1);
          assertThat(searchNode.getValue().iterator().next()).isEqualTo(snapshot2Metadata.name);
        } else {
          fail(
              "SearchNodes should only query cache1 and cache2 but is trying to search "
                  + searchNode.getKey());
        }
      }
    }
  }

  @Test
  public void testMultipleDatasetsMultipleTimeRange() throws Exception {

    // dataset1 snapshots/search-metadata/partitions
    SnapshotMetadata snapshotMetadata =
        createSnapshot(Instant.ofEpochMilli(100), Instant.ofEpochMilli(200), false, "1");
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 1);
    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache1SearchContext, snapshotMetadata.name);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 1);

    snapshotMetadata =
        createSnapshot(Instant.ofEpochMilli(201), Instant.ofEpochMilli(300), false, "2");
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 2);
    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache2SearchContext, snapshotMetadata.name);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 2);

    final String name = "testDataset";
    final String owner = "DatasetOwner";
    final long throughputBytes = 1000;
    final DatasetPartitionMetadata partition11 =
        new DatasetPartitionMetadata(100, 200, List.of("1"));
    final DatasetPartitionMetadata partition12 =
        new DatasetPartitionMetadata(201, 300, List.of("2"));

    DatasetMetadata datasetMetadata =
        new DatasetMetadata(name, owner, throughputBytes, List.of(partition11, partition12), name);

    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    // dataset2 snapshots/search-metadata/partitions
    snapshotMetadata =
        createSnapshot(Instant.ofEpochMilli(100), Instant.ofEpochMilli(200), false, "2");
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 3);
    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache3SearchContext, snapshotMetadata.name);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 3);

    snapshotMetadata =
        createSnapshot(Instant.ofEpochMilli(201), Instant.ofEpochMilli(300), false, "1");
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 4);
    ReadOnlyChunkImpl.registerSearchMetadata(
        searchMetadataStore, cache4SearchContext, snapshotMetadata.name);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 4);

    final String name1 = "testDataset1";
    final String owner1 = "DatasetOwner1";
    final long throughputBytes1 = 1;
    final DatasetPartitionMetadata partition21 =
        new DatasetPartitionMetadata(100, 200, List.of("2"));
    final DatasetPartitionMetadata partition22 =
        new DatasetPartitionMetadata(201, 300, List.of("1"));

    DatasetMetadata datasetMetadata1 =
        new DatasetMetadata(
            name1, owner1, throughputBytes1, List.of(partition21, partition22), name1);
    datasetMetadataStore.createSync(datasetMetadata1);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 2);

    // find search nodes that will be queries for the first dataset
    Map<String, List<String>> searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore, searchMetadataStore, datasetMetadataStore, 100, 199, name);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(cache1SearchContext.toString());

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore, searchMetadataStore, datasetMetadataStore, 100, 299, name);
    assertThat(searchNodes.size()).isEqualTo(2);
    Iterator<String> iter = searchNodes.keySet().iterator();
    String node1 = iter.next();
    String node2 = iter.next();
    assertThat(
            node1.equals(cache1SearchContext.toString())
                || node1.equals(cache2SearchContext.toString()))
        .isTrue();
    assertThat(
            node2.equals(cache1SearchContext.toString())
                || node2.equals(cache2SearchContext.toString()))
        .isTrue();

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore, searchMetadataStore, datasetMetadataStore, 100, 299, name1);
    assertThat(searchNodes.size()).isEqualTo(2);
    iter = searchNodes.keySet().iterator();
    node1 = iter.next();
    node2 = iter.next();
    assertThat(
            node1.equals(cache3SearchContext.toString())
                || node1.equals(cache4SearchContext.toString()))
        .isTrue();
    assertThat(
            node2.equals(cache3SearchContext.toString())
                || node2.equals(cache4SearchContext.toString()))
        .isTrue();
  }

  @Test
  public void testTwoIndexerWithDifferentPartitions() {
    String indexName1 = "testIndex1";
    String indexName2 = "testIndex2";
    // search for partition "1" only
    Instant chunkCreationTime = Instant.ofEpochMilli(100);
    Instant chunkEndTime = Instant.ofEpochMilli(200);
    createIndexerZKMetadata(chunkCreationTime, chunkEndTime, "1", indexer1SearchContext);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 2);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 1);

    DatasetPartitionMetadata partition = new DatasetPartitionMetadata(1, 200, List.of("1"));
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(indexName1, "testOwner1", 1, List.of(partition), indexName1);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);

    Map<String, List<String>> searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            indexName1);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer1SearchContext.toString());

    // search for partition "2" only
    createIndexerZKMetadata(chunkCreationTime, chunkEndTime, "2", indexer2SearchContext);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 4);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 2);

    partition = new DatasetPartitionMetadata(1, 101, List.of("2"));
    datasetMetadata =
        new DatasetMetadata(indexName2, "testOwner2", 1, List.of(partition), indexName2);
    datasetMetadataStore.createSync(datasetMetadata);
    await().until(() -> KaldbMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 2);

    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            indexName2);
    assertThat(searchNodes.size()).isEqualTo(1);
    assertThat(searchNodes.keySet().iterator().next()).isEqualTo(indexer2SearchContext.toString());

    // search for wrong indexName and see if you get 0 nodes
    searchNodes =
        getSearchNodesToQuery(
            snapshotMetadataStore,
            searchMetadataStore,
            datasetMetadataStore,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            "new_dataset_that_does_not_have_a_partition");
    assertThat(searchNodes.size()).isEqualTo(0);
  }

  @Test
  public void testSchema() {
    Instant endTime = Instant.now();
    Instant startTime = endTime.minus(1, ChronoUnit.HOURS);

    // mock the zookeeper responses, as we don't want to test ZK behavior here
    SearchMetadataStore searchMetadataStoreMock = mock(SearchMetadataStore.class);
    when(searchMetadataStoreMock.listSync())
        .thenReturn(List.of(new SearchMetadata("foo", "snapshot1", "http://127.0.0.1")));
    SnapshotMetadataStore snapshotMetadataStoreMock = mock(SnapshotMetadataStore.class);
    when(snapshotMetadataStoreMock.listSync())
        .thenReturn(
            List.of(
                new SnapshotMetadata(
                    "snapshot1",
                    "/1",
                    endTime.minus(30, ChronoUnit.MINUTES).toEpochMilli(),
                    endTime.toEpochMilli(),
                    10,
                    "1",
                    Metadata.IndexType.LOGS_LUCENE9)));
    DatasetMetadataStore datasetMetadataStoreMock = mock(DatasetMetadataStore.class);
    when(datasetMetadataStoreMock.listSync())
        .thenReturn(
            List.of(
                new DatasetMetadata(
                    "foo",
                    "foo",
                    10,
                    List.of(
                        new DatasetPartitionMetadata(
                            endTime.minus(1, ChronoUnit.DAYS).toEpochMilli(),
                            Long.MAX_VALUE,
                            List.of("1"))),
                    "")));

    KaldbDistributedQueryService distributedQueryService =
        new KaldbDistributedQueryService(
            searchMetadataStoreMock,
            snapshotMetadataStoreMock,
            datasetMetadataStoreMock,
            new SimpleMeterRegistry(),
            Duration.of(2, ChronoUnit.SECONDS),
            Duration.of(2, ChronoUnit.SECONDS));

    // Make a mock grpc stub and store it in the stubs cache map
    KaldbServiceGrpc.KaldbServiceFutureStub futureStub =
        mock(KaldbServiceGrpc.KaldbServiceFutureStub.class);
    distributedQueryService.stubs.put("http://127.0.0.1", futureStub);
    when(futureStub.withDeadlineAfter(anyLong(), any(TimeUnit.class))).thenReturn(futureStub);
    when(futureStub.withInterceptors(any())).thenReturn(futureStub);
    when(futureStub.schema(any(KaldbSearch.SchemaRequest.class)))
        .thenReturn(
            Futures.immediateFuture(
                KaldbSearch.SchemaResult.newBuilder()
                    .putFieldDefinition(
                        "foo",
                        KaldbSearch.SchemaDefinition.newBuilder()
                            .setType(Schema.SchemaFieldType.TEXT)
                            .build())
                    .build()));

    // wildcard schema query should return a result
    KaldbSearch.SchemaRequest schemaRequestWildcard =
        KaldbSearch.SchemaRequest.newBuilder()
            .setStartTimeEpochMs(startTime.toEpochMilli())
            .setEndTimeEpochMs(endTime.toEpochMilli())
            .setDataset("*")
            .build();
    KaldbSearch.SchemaResult schemaResultWildcard =
        distributedQueryService.getSchema(schemaRequestWildcard);
    assertThat(schemaResultWildcard.getFieldDefinitionMap())
        .isEqualTo(
            Map.of(
                "foo",
                KaldbSearch.SchemaDefinition.newBuilder()
                    .setType(Schema.SchemaFieldType.TEXT)
                    .build()));

    // Exact dataset query should return a result
    KaldbSearch.SchemaRequest schemaRequestExact =
        KaldbSearch.SchemaRequest.newBuilder()
            .setStartTimeEpochMs(startTime.toEpochMilli())
            .setEndTimeEpochMs(endTime.toEpochMilli())
            .setDataset("foo")
            .build();
    KaldbSearch.SchemaResult schemaResultExact =
        distributedQueryService.getSchema(schemaRequestExact);
    assertThat(schemaResultExact.getFieldDefinitionMap())
        .isEqualTo(
            Map.of(
                "foo",
                KaldbSearch.SchemaDefinition.newBuilder()
                    .setType(Schema.SchemaFieldType.TEXT)
                    .build()));

    // query window that returns no results should have no schema
    KaldbSearch.SchemaRequest schemaRequestMissing =
        KaldbSearch.SchemaRequest.newBuilder()
            .setStartTimeEpochMs(0)
            .setEndTimeEpochMs(10)
            .setDataset("*")
            .build();
    KaldbSearch.SchemaResult schemaResultMissing =
        distributedQueryService.getSchema(schemaRequestMissing);
    assertThat(schemaResultMissing.getFieldDefinitionMap().size()).isEqualTo(0);

    // query window that queries a different dataset should have no schema
    KaldbSearch.SchemaRequest schemaRequestWrongDataset =
        KaldbSearch.SchemaRequest.newBuilder()
            .setStartTimeEpochMs(startTime.toEpochMilli())
            .setEndTimeEpochMs(endTime.toEpochMilli())
            .setDataset("bar")
            .build();
    KaldbSearch.SchemaResult schemaResultWrongDataset =
        distributedQueryService.getSchema(schemaRequestWrongDataset);
    assertThat(schemaResultWrongDataset.getFieldDefinitionMap().size()).isEqualTo(0);
    distributedQueryService.close();
  }

  private String createIndexerZKMetadata(
      Instant chunkCreationTime,
      Instant chunkEndTime,
      String partition,
      SearchContext searchContext) {
    SnapshotMetadata liveSnapshotMetadata =
        createSnapshot(chunkCreationTime, chunkEndTime, true, partition);
    SearchMetadata liveSearchMetadata = toSearchMetadata(liveSnapshotMetadata.name, searchContext);

    searchMetadataStore.createSync(liveSearchMetadata);

    return liveSnapshotMetadata.name.substring(
        5); // remove LIVE_ prefix for a search metadata hosted by a cache node
  }

  private SnapshotMetadata createSnapshot(
      Instant chunkCreationTime, Instant chunkEndTime, boolean isLive, String partition) {
    String chunkName = "logStore_" + chunkCreationTime.getEpochSecond() + "_" + UUID.randomUUID();
    ChunkInfo chunkInfo =
        new ChunkInfo(
            chunkName,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            1234,
            partition,
            isLive ? LIVE_SNAPSHOT_PATH : "cacheSnapshotPath");
    SnapshotMetadata snapshotMetadata =
        toSnapshotMetadata(chunkInfo, isLive ? LIVE_SNAPSHOT_PREFIX : "");

    snapshotMetadataStore.createSync(snapshotMetadata);

    if (isLive) {
      // create non-live as well to simulate postClose of IndexingChunkImpl
      SnapshotMetadata nonLiveSnapshotMetadata = toSnapshotMetadata(chunkInfo, "");
      snapshotMetadataStore.createSync(nonLiveSnapshotMetadata);
    }

    return snapshotMetadata;
  }

  private Map<String, List<String>> getSearchNodesToQuery(
      SnapshotMetadataStore snapshotMetadataStore,
      SearchMetadataStore searchMetadataStore,
      DatasetMetadataStore datasetMetadataStore,
      long queryStartTimeEpochMs,
      long queryEndTimeEpochMs,
      String dataset) {
    Map<String, SnapshotMetadata> snapshotsToSearch =
        getMatchingSnapshots(
            snapshotMetadataStore,
            datasetMetadataStore,
            queryStartTimeEpochMs,
            queryEndTimeEpochMs,
            dataset);

    Map<String, List<SearchMetadata>> searchMetadataToQuery =
        getMatchingSearchMetadata(searchMetadataStore, snapshotsToSearch);

    return getNodesAndSnapshotsToQuery(searchMetadataToQuery);
  }
}
