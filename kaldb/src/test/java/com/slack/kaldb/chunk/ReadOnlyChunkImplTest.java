package com.slack.kaldb.chunk;

import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.testlib.MessageUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class ReadOnlyChunkImplTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private MeterRegistry registry;
  private String localIndexPath;
  private SearchQuery searchQuery;

  @Before
  public void setUp() throws IOException {
    Tracing.newBuilder().build();
    registry = new SimpleMeterRegistry();
    File localStore = temporaryFolder.newFolder();

    // Create a lucene index for reads.
    LuceneIndexStoreImpl logStore =
        LuceneIndexStoreImpl.makeLogStore(
            localStore, Duration.ofSeconds(5 * 60), Duration.ofSeconds(5 * 60), registry);
    ReadWriteChunkImpl<LogMessage> chunk =
        new ReadWriteChunkImpl<>(logStore, "testDataSet", registry);
    localIndexPath = logStore.getDirectory().toAbsolutePath().toString();

    // Add messages to the store using a ReadWriteChunkImpl.
    List<LogMessage> messages = MessageUtil.makeMessagesWithTimeDifference(1, 100);
    for (LogMessage m : messages) {
      chunk.addMessage(m);
    }
    chunk.commit();

    // Search the chunk to make sure this works
    searchQuery = new SearchQuery(MessageUtil.TEST_INDEX_NAME, "Message1", 0, MAX_TIME, 10, 1000);
    SearchResult<LogMessage> results = chunk.query(searchQuery);
    assertThat(results.hits.size()).isEqualTo(1);

    chunk.close();
  }

  @After
  public void tearDown() {
    registry.close();
  }
}
