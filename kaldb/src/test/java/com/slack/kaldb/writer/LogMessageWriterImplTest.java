package com.slack.kaldb.writer;

import static com.slack.kaldb.bulkIngestApi.opensearch.BulkApiRequestParser.convertRequestToDocument;
import static com.slack.kaldb.bulkIngestApi.opensearch.BulkApiRequestParserTest.getIndexRequestBytes;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.testlib.ChunkManagerUtil.makeChunkManagerUtil;
import static com.slack.kaldb.testlib.MessageUtil.TEST_MESSAGE_TYPE;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.SpanUtil.makeSpan;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherExtension.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.slack.kaldb.bulkIngestApi.opensearch.BulkApiRequestParser;
import com.slack.kaldb.chunkManager.IndexingChunkManager;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.metadata.schema.SchemaUtil;
import com.slack.kaldb.proto.schema.Schema;
import com.slack.kaldb.testlib.ChunkManagerUtil;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.ingest.IngestDocument;

public class LogMessageWriterImplTest {

  private static final String S3_TEST_BUCKET = "test-kaldb-logs";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(S3_TEST_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  private ChunkManagerUtil<LogMessage> chunkManagerUtil;
  private SimpleMeterRegistry metricsRegistry;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        makeChunkManagerUtil(
            S3_MOCK_EXTENSION,
            S3_TEST_BUCKET,
            metricsRegistry,
            10 * 1024 * 1024 * 1024L,
            100,
            KaldbConfigUtil.makeIndexerConfig());
    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
  }

  @AfterEach
  public void tearDown() throws IOException, TimeoutException {
    if (chunkManagerUtil != null) {
      chunkManagerUtil.close();
    }
    metricsRegistry.close();
  }

  private SearchResult<LogMessage> searchChunkManager(String indexName, String queryString) {
    return chunkManagerUtil.chunkManager.query(
        new SearchQuery(
            indexName,
            queryString,
            0,
            MAX_TIME,
            10,
            new DateHistogramAggBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"),
            Collections.emptyList()),
        Duration.ofMillis(3000));
  }

  private static ConsumerRecord<String, byte[]> consumerRecordWithValue(byte[] recordValue) {
    return new ConsumerRecord<>(
        "testTopic", 1, 10, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, "testKey", recordValue);
  }

  @Test
  public void insertNullRecord() throws IOException {
    LogMessageWriterImpl messageWriter = new LogMessageWriterImpl(chunkManagerUtil.chunkManager);

    assertThat(messageWriter.insertRecord(null)).isFalse();
  }

  // TODO: Add a unit test where message fails to index. Can't do it now since the field conflict
  // policy is hard-coded.

  @Test
  public void testAvgMessageSizeCalculationOnSpanIngestion() throws Exception {
    final String traceId = "t1";
    final Instant timestamp = Instant.now();
    final long durationMicros = 500000L;
    final String serviceName = "test_service";
    final String name = "testSpanName";

    SimpleMeterRegistry localMetricsRegistry = new SimpleMeterRegistry();
    ChunkManagerUtil<LogMessage> localChunkManagerUtil =
        makeChunkManagerUtil(
            S3_MOCK_EXTENSION,
            S3_TEST_BUCKET,
            localMetricsRegistry,
            1000L,
            100,
            KaldbConfigUtil.makeIndexerConfig());
    localChunkManagerUtil.chunkManager.startAsync();
    localChunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);

    List<Trace.Span> spans =
        IntStream.range(0, 15)
            .mapToObj(
                i ->
                    makeSpan(
                        traceId,
                        String.valueOf(i),
                        "0",
                        TimeUnit.MICROSECONDS.convert(
                            timestamp.toEpochMilli() + i * 1000L, TimeUnit.MILLISECONDS),
                        durationMicros,
                        name,
                        serviceName,
                        TEST_MESSAGE_TYPE))
            .collect(Collectors.toList());

    IndexingChunkManager<LogMessage> chunkManager = localChunkManagerUtil.chunkManager;
    LogMessageWriterImpl messageWriter = new LogMessageWriterImpl(chunkManager);

    for (Trace.Span span : spans) {
      ConsumerRecord<String, byte[]> spanRecord = consumerRecordWithValue(span.toByteArray());
      assertThat(messageWriter.insertRecord(spanRecord)).isTrue();
    }

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, localMetricsRegistry)).isEqualTo(15);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, localMetricsRegistry)).isEqualTo(0);
    localChunkManagerUtil.chunkManager.getActiveChunk().commit();

    // even though we may have over 1k bytes, we only update the bytes ingested every 10s
    // as such this will only have 1 chunk at the time of query
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);

    assertThat(
            chunkManager
                .query(
                    new SearchQuery(
                        serviceName,
                        "",
                        0,
                        MAX_TIME,
                        100,
                        new DateHistogramAggBuilder(
                            "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"),
                        Collections.emptyList()),
                    Duration.ofMillis(3000))
                .hits
                .size())
        .isEqualTo(15);
  }

  @Test
  public void testIngestTraceSpan() throws IOException {
    final String traceId = "t1";
    final String id = "i1";
    final String parentId = "p2";
    final Instant timestamp = Instant.now();
    final long durationMicros = 500000L;
    final String serviceName = "test_service";
    final String name = "testSpanName";
    final String msgType = "test_message_type";
    final Trace.Span span =
        makeSpan(
            traceId,
            id,
            parentId,
            TimeUnit.MICROSECONDS.convert(timestamp.toEpochMilli(), TimeUnit.MILLISECONDS),
            durationMicros,
            name,
            serviceName,
            msgType);
    ConsumerRecord<String, byte[]> spanRecord = consumerRecordWithValue(span.toByteArray());

    LogMessageWriterImpl messageWriter = new LogMessageWriterImpl(chunkManagerUtil.chunkManager);

    assertThat(messageWriter.insertRecord(spanRecord)).isTrue();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManagerUtil.chunkManager.getActiveChunk().commit();

    assertThat(searchChunkManager(serviceName, "").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "http_method:POST").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "type:test_message_type").hits.size()).isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "service_name:test_service").hits.size())
        .isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "http_method:GET").hits.size()).isEqualTo(0);
    assertThat(searchChunkManager(serviceName, "method:callbacks*").hits.size()).isEqualTo(1);
    assertThat(
            searchChunkManager(serviceName, "http_method:POST AND method:callbacks*").hits.size())
        .isEqualTo(1);
    assertThat(searchChunkManager(serviceName, "http_method:GET AND method:callbacks*").hits.size())
        .isEqualTo(0);
    assertThat(searchChunkManager(serviceName, "http_method:GET OR method:callbacks*").hits.size())
        .isEqualTo(1);
  }

  @Test
  public void parseAndIndexBulkApiRequestTest() throws IOException {
    // crux of the test - encoding and decoding of binary fields
    //    ByteString inputBytes = ByteString.copyFrom("{\"key1\":
    // \"value1\"}".toString().getBytes());
    //
    //    String output = SpanFormatter.encodeBinaryTagValue(inputBytes);
    //    System.out.println(output);

    String inputDocuments =
        """
    { "index" : { "_index" : "test", "_id" : "1" } }
    { "field1" : "value1", "field2" : "value2", "tags" :  [] }
    { "index" : { "_index" : "test", "_id" : "2" } }
    { "field1" : "value1", "field2" : "value2", "tags" :  ["tagValue1", "tagValue2"] }
    { "index" : { "_index" : "test", "_id" : "3" } }
    { "field1" : "value1", "field2" : "value2", "message" :  {} }
    { "index" : { "_index" : "test", "_id" : "4" } }
    { "field1" : "value1", "field2" : "value2", "message" :  { "nestedField1" : "nestedValue1", "nestedField2" : "nestedValue2" } }
            """;

    byte[] rawRequest = inputDocuments.getBytes(StandardCharsets.UTF_8);

    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(4);

    for (IndexRequest indexRequest : indexRequests) {
      IngestDocument ingestDocument = convertRequestToDocument(indexRequest);
      Trace.Span span =
          BulkApiRequestParser.fromIngestDocument(
              ingestDocument, Schema.IngestSchema.newBuilder().build());
      ConsumerRecord<String, byte[]> spanRecord = consumerRecordWithValue(span.toByteArray());
      LogMessageWriterImpl messageWriter = new LogMessageWriterImpl(chunkManagerUtil.chunkManager);
      assertThat(messageWriter.insertRecord(spanRecord)).isTrue();
    }

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(4);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManagerUtil.chunkManager.getActiveChunk().commit();

    SearchResult<LogMessage> results = searchChunkManager("test", "_id:1");
    assertThat(results.hits.size()).isEqualTo(1);
    Object value = results.hits.get(0).getSource().get("tags");
    assertThat(value).isEqualTo("[]");

    results = searchChunkManager("test", "_id:2");
    assertThat(results.hits.size()).isEqualTo(1);
    value = results.hits.get(0).getSource().get("tags");
    // ArrayList#toString in SpanFormatter#convertKVtoProto for the binary field type case
    assertThat(value).isEqualTo("[tagValue1, tagValue2]");

    results = searchChunkManager("test", "_id:3");
    assertThat(results.hits.size()).isEqualTo(1);
    value = results.hits.get(0).getSource().get("message");
    assertThat(value).isEqualTo("{}");

    results = searchChunkManager("test", "_id:4");
    assertThat(results.hits.size()).isEqualTo(1);
    value = results.hits.get(0).getSource().get("message");
    // HashMap#toString in SpanFormatter#convertKVtoProto for the binary field type case
    assertThat(value).isEqualTo("{nestedField2=nestedValue2, nestedField1=nestedValue1}");
  }

  @Test
  public void indexAndSearchAllFieldTypes() throws IOException {
    final File schemaFile =
        new File(getClass().getClassLoader().getResource("schema/test_schema.yaml").getFile());
    Schema.IngestSchema schema = SchemaUtil.parseSchema(schemaFile.toPath());

    byte[] rawRequest = getIndexRequestBytes("index_all_schema_fields");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    AssertionsForClassTypes.assertThat(indexRequests.size()).isEqualTo(1);

    for (IndexRequest indexRequest : indexRequests) {
      IngestDocument ingestDocument = convertRequestToDocument(indexRequest);
      Trace.Span span = BulkApiRequestParser.fromIngestDocument(ingestDocument, schema);
      ConsumerRecord<String, byte[]> spanRecord = consumerRecordWithValue(span.toByteArray());
      LogMessageWriterImpl messageWriter = new LogMessageWriterImpl(chunkManagerUtil.chunkManager);
      assertThat(messageWriter.insertRecord(spanRecord)).isTrue();
    }

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManagerUtil.chunkManager.getActiveChunk().commit();

    SearchResult<LogMessage> results = searchChunkManager("test", "_id:1");
    assertThat(results.hits.size()).isEqualTo(1);

    // message field
    results = searchChunkManager("test", "message:foo");
    assertThat(results.hits.size()).isEqualTo(1);
    results = searchChunkManager("test", "message:bar");
    assertThat(results.hits.size()).isEqualTo(1);
    results = searchChunkManager("test", "message:\"foo bar\"");
    assertThat(results.hits.size()).isEqualTo(1);

    results = searchChunkManager("test", "ip:0.0.0.0");
    assertThat(results.hits.size()).isEqualTo(1);
    results = searchChunkManager("test", "ip:[0.0.0.0 TO 0.0.0.1]");
    assertThat(results.hits.size()).isEqualTo(1);

    results = searchChunkManager("test", "my_date:\"2014-09-01T12:00:00Z\"");
    assertThat(results.hits.size()).isEqualTo(1);
    results =
        searchChunkManager(
            "test", "my_date:[\"2014-09-01T12:00:00Z\" TO \"2014-09-01T12:00:00Z\"]");
    assertThat(results.hits.size()).isEqualTo(1);

    results = searchChunkManager("test", "success:true");
    assertThat(results.hits.size()).isEqualTo(1);

    results = searchChunkManager("test", "cost:4.0");
    assertThat(results.hits.size()).isEqualTo(1);
    results = searchChunkManager("test", "cost:[0 TO 4.0]");
    assertThat(results.hits.size()).isEqualTo(1);
    results = searchChunkManager("test", "cost:[4 TO 4.1]");
    assertThat(results.hits.size()).isEqualTo(1);

    results = searchChunkManager("test", "amount:1.1");
    assertThat(results.hits.size()).isEqualTo(1);
    results = searchChunkManager("test", "amount:[1 TO 1.1]");
    assertThat(results.hits.size()).isEqualTo(1);

    results = searchChunkManager("test", "amount_half_float:1.2");
    assertThat(results.hits.size()).isEqualTo(1);
    results = searchChunkManager("test", "amount_half_float:[1.0 TO 1.3]");
    assertThat(results.hits.size()).isEqualTo(1);

    results = searchChunkManager("test", "value:42");
    assertThat(results.hits.size()).isEqualTo(1);
    results = searchChunkManager("test", "value:[0 TO 42]");
    assertThat(results.hits.size()).isEqualTo(1);

    results = searchChunkManager("test", "count:3");
    assertThat(results.hits.size()).isEqualTo(1);
    results = searchChunkManager("test", "count:[3 TO 42]");
    assertThat(results.hits.size()).isEqualTo(1);

    results = searchChunkManager("test", "count_scaled_long:80");
    assertThat(results.hits.size()).isEqualTo(1);
    results = searchChunkManager("test", "count_scaled_long:[79 TO 81}");
    assertThat(results.hits.size()).isEqualTo(1);

    results = searchChunkManager("test", "count_short:10");
    assertThat(results.hits.size()).isEqualTo(1);
    results = searchChunkManager("test", "count_short:{9 TO 10]");
    assertThat(results.hits.size()).isEqualTo(1);

    results = searchChunkManager("test", "bucket:20");
    assertThat(results.hits.size()).isEqualTo(1);
    results = searchChunkManager("test", "bucket:[0 TO 20]");
    assertThat(results.hits.size()).isEqualTo(1);
  }

  @Test
  public void testNullTraceSpan() throws IOException {
    LogMessageWriterImpl messageWriter = new LogMessageWriterImpl(chunkManagerUtil.chunkManager);

    assertThat(messageWriter.insertRecord(null)).isFalse();
  }
}
