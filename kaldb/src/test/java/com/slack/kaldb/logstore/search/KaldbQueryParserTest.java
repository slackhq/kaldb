package com.slack.kaldb.logstore.search;

import static com.slack.kaldb.testlib.MessageUtil.TEST_DATASET_NAME;
import static com.slack.kaldb.testlib.MessageUtil.TEST_MESSAGE_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherRule;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class KaldbQueryParserTest {

  @Rule
  public TemporaryLogStoreAndSearcherRule strictLogStore =
      new TemporaryLogStoreAndSearcherRule(false, true);

  public KaldbQueryParserTest() throws IOException {}

  @BeforeClass
  public static void initTests() {
    Tracing.newBuilder().build();
  }

  @Test
  public void testExistsQuery() {
    Instant time = Instant.now();
    strictLogStore.logStore.addMessage(makeMessageForExistsSearch("testIndex", "1", "test", time));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
    SearchResult<LogMessage> result =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            "service_name:test",
            time.toEpochMilli(),
            time.plusSeconds(1).toEpochMilli(),
            100,
            0);
    assertThat(result.hits.size()).isEqualTo(1);
    assertThat(result.totalCount).isEqualTo(1);
    assertThat(result.buckets.size()).isEqualTo(0);

    String queryStr = LogMessage.ReservedField.SERVICE_NAME.fieldName + ":*";
    result =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            queryStr,
            time.toEpochMilli(),
            time.plusSeconds(1).toEpochMilli(),
            100,
            0);
    assertThat(result.hits.size()).isEqualTo(1);
    assertThat(result.totalCount).isEqualTo(1);
    assertThat(result.buckets.size()).isEqualTo(0);

    queryStr = "_exists_:" + LogMessage.ReservedField.SERVICE_NAME.fieldName;
    result =
        strictLogStore.logSearcher.search(
            TEST_DATASET_NAME,
            queryStr,
            time.toEpochMilli(),
            time.plusSeconds(1).toEpochMilli(),
            100,
            0);
    assertThat(result.hits.size()).isEqualTo(1);
    assertThat(result.totalCount).isEqualTo(1);
    assertThat(result.buckets.size()).isEqualTo(0);
  }

  private static LogMessage makeMessageForExistsSearch(
      String indexName, String id, String message, Instant ts) {
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put(LogMessage.ReservedField.TIMESTAMP.fieldName, ts.toString());
    fieldMap.put(LogMessage.ReservedField.SERVICE_NAME.fieldName, message);
    LogWireMessage wireMsg = new LogWireMessage(indexName, TEST_MESSAGE_TYPE, id, fieldMap);
    return LogMessage.fromWireMessage(wireMsg);
  }
}
