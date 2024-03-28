package com.slack.kaldb.logstore;

import static com.slack.kaldb.logstore.BlobFsUtils.DELIMITER;
import static com.slack.kaldb.logstore.BlobFsUtils.copyFromS3;
import static com.slack.kaldb.logstore.BlobFsUtils.copyToLocalPath;
import static com.slack.kaldb.logstore.BlobFsUtils.copyToS3;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.COMMITS_TIMER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.kaldb.logstore.LuceneIndexStoreImpl.REFRESHES_TIMER;
import static com.slack.kaldb.testlib.MetricsUtil.getCount;
import static com.slack.kaldb.testlib.MetricsUtil.getTimerCount;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherExtension.MAX_TIME;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherExtension.addMessages;
import static com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherExtension.findAllMessages;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.google.protobuf.ByteString;
import com.slack.kaldb.blobfs.LocalBlobFs;
import com.slack.kaldb.blobfs.s3.S3CrtBlobFs;
import com.slack.kaldb.blobfs.s3.S3TestUtils;
import com.slack.kaldb.logstore.LogMessage.ReservedField;
import com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.kaldb.logstore.search.LogIndexSearcherImpl;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.proto.schema.Schema;
import com.slack.kaldb.testlib.MessageUtil;
import com.slack.kaldb.testlib.SpanUtil;
import com.slack.kaldb.testlib.TemporaryLogStoreAndSearcherExtension;
import com.slack.service.murron.trace.Trace;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.index.IndexCommit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

@SuppressWarnings("unused")
public class LuceneIndexStoreImplTest {
  @BeforeAll
  public static void beforeClass() {
    Tracing.newBuilder().build();
  }

  @Nested
  public class TestsWithConvertAndDuplicateFieldPolicy {
    @RegisterExtension
    public TemporaryLogStoreAndSearcherExtension logStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    public TestsWithConvertAndDuplicateFieldPolicy() throws IOException {}

    @Test
    public void testSimpleIndexAndQuery() {
      addMessages(logStore.logStore, 1, 100, true);
      Collection<LogMessage> results =
          findAllMessages(logStore.logSearcher, MessageUtil.TEST_DATASET_NAME, "Message1", 10);
      assertThat(results.size()).isEqualTo(1);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, logStore.metricsRegistry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, logStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, logStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, logStore.metricsRegistry)).isEqualTo(1);
    }

    @Test
    public void testSearchAndQueryDocsWithNestedJson() {
      Trace.Span span =
          Trace.Span.newBuilder()
              .setId(ByteString.copyFromUtf8("1"))
              .addTags(
                  Trace.KeyValue.newBuilder()
                      .setVStr("Test message")
                      .setKey("message")
                      .setFieldType(Schema.SchemaFieldType.KEYWORD)
                      .build())
              .addTags(
                  Trace.KeyValue.newBuilder()
                      .setVStr("duplicate1")
                      .setKey("duplicateproperty")
                      .setFieldType(Schema.SchemaFieldType.KEYWORD)
                      .build())
              .addTags(
                  Trace.KeyValue.newBuilder()
                      .setVStr("value1")
                      .setKey("nested.key1")
                      .setFieldType(Schema.SchemaFieldType.KEYWORD)
                      .build())
              .addTags(
                  Trace.KeyValue.newBuilder()
                      .setVStr("2")
                      .setKey("nested.duplicateproperty")
                      .setFieldType(Schema.SchemaFieldType.KEYWORD)
                      .build())
              .build();
      logStore.logStore.addMessage(span);
      logStore.logStore.commit();
      logStore.logStore.refresh();

      SearchResult<LogMessage> result1 =
          logStore.logSearcher.search(
              MessageUtil.TEST_DATASET_NAME,
              "nested.key1:value1",
              0L,
              MAX_TIME,
              100,
              new DateHistogramAggBuilder(
                  "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
      assertThat(result1.hits.size()).isEqualTo(1);

      SearchResult<LogMessage> result2 =
          logStore.logSearcher.search(
              MessageUtil.TEST_DATASET_NAME,
              "duplicateproperty:duplicate1",
              0L,
              MAX_TIME,
              100,
              new DateHistogramAggBuilder(
                  "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
      assertThat(result2.hits.size()).isEqualTo(1);

      SearchResult<LogMessage> result3 =
          logStore.logSearcher.search(
              MessageUtil.TEST_DATASET_NAME,
              "nested.duplicateproperty:2",
              0L,
              MAX_TIME,
              100,
              new DateHistogramAggBuilder(
                  "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "1s"));
      assertThat(result3.hits.size()).isEqualTo(1);
    }

    @Test
    public void testQueryReturnsMultipleHits() {
      addMessages(logStore.logStore, 1, 100, true);
      Collection<LogMessage> results =
          findAllMessages(logStore.logSearcher, MessageUtil.TEST_DATASET_NAME, "identifier", 1000);
      assertThat(results.size()).isEqualTo(100);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, logStore.metricsRegistry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, logStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, logStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, logStore.metricsRegistry)).isEqualTo(1);
    }

    @Test
    public void testTimestampOrdering() {
      addMessages(logStore.logStore, 1, 100, true);
      List<LogMessage> results =
          findAllMessages(logStore.logSearcher, MessageUtil.TEST_DATASET_NAME, "identifier", 1);
      assertThat(results.size()).isEqualTo(1);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, logStore.metricsRegistry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, logStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, logStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, logStore.metricsRegistry)).isEqualTo(1);
      assertThat(results.get(0).getId()).isEqualTo("Message100");
    }

    @Test
    public void testIndexDocsWithTypeMismatchErrors() {
      Trace.KeyValue wrongField =
          Trace.KeyValue.newBuilder()
              .setKey(ReservedField.HOSTNAME.fieldName)
              .setVInt32(1)
              .setFieldType(Schema.SchemaFieldType.INTEGER)
              .build();
      logStore.logStore.addMessage(
          SpanUtil.makeSpan(100, "test", Instant.now(), List.of(wrongField)));
      addMessages(logStore.logStore, 1, 99, true);
      Collection<LogMessage> results =
          findAllMessages(logStore.logSearcher, MessageUtil.TEST_DATASET_NAME, "identifier", 1000);
      assertThat(results.size()).isEqualTo(99);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, logStore.metricsRegistry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, logStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, logStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, logStore.metricsRegistry)).isEqualTo(1);
    }
  }

  @Nested
  public class TestsWithRaiseErrorFieldConflictPolicy {
    @RegisterExtension
    public TemporaryLogStoreAndSearcherExtension logStore =
        new TemporaryLogStoreAndSearcherExtension(
            Duration.of(5, ChronoUnit.MINUTES),
            Duration.of(5, ChronoUnit.MINUTES),
            true,
            SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.RAISE_ERROR);

    public TestsWithRaiseErrorFieldConflictPolicy() throws IOException {}

    @Test
    public void failIndexingDocsWithMismatchedTypeErrors() {
      Trace.KeyValue wrongField =
          Trace.KeyValue.newBuilder()
              .setKey(ReservedField.HOSTNAME.fieldName)
              .setFieldType(Schema.SchemaFieldType.INTEGER)
              .setVInt32(20000)
              .build();

      logStore.logStore.addMessage(
          SpanUtil.makeSpan(100, "test", Instant.now(), List.of(wrongField)));
      addMessages(logStore.logStore, 1, 99, true);
      Collection<LogMessage> results =
          findAllMessages(logStore.logSearcher, MessageUtil.TEST_DATASET_NAME, "identifier", 1000);
      assertThat(results.size()).isEqualTo(99);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, logStore.metricsRegistry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, logStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(REFRESHES_TIMER, logStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, logStore.metricsRegistry)).isEqualTo(1);
    }

    @Test
    public void indexLongUnbreakableField() {
      String hugeField =
          IntStream.range(1, 10000).boxed().map(String::valueOf).collect(Collectors.joining(""));

      Trace.KeyValue hugeFieldTag =
          Trace.KeyValue.newBuilder().setKey("hugefield").setVStr(hugeField).build();

      logStore.logStore.addMessage(
          SpanUtil.makeSpan(1, "Test message", Instant.now(), List.of(hugeFieldTag)));
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, logStore.metricsRegistry)).isEqualTo(1);
      // UTF8 encoding is longer than the max length 32766
      assertThat(getCount(MESSAGES_FAILED_COUNTER, logStore.metricsRegistry)).isEqualTo(1);
      // Counters not set since no commit
      assertThat(getTimerCount(REFRESHES_TIMER, logStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(COMMITS_TIMER, logStore.metricsRegistry)).isEqualTo(0);
    }

    @Test
    public void testFieldSearch() {
      Trace.KeyValue hostField =
          Trace.KeyValue.newBuilder()
              .setKey(ReservedField.HOSTNAME.fieldName)
              .setVStr("host1-dc2.abc.com")
              .build();

      Trace.KeyValue tagField =
          Trace.KeyValue.newBuilder()
              .setKey(ReservedField.TAG.fieldName)
              .setVStr("foo-bar")
              .build();

      logStore.logStore.addMessage(
          SpanUtil.makeSpan(1, "Test message", Instant.now(), List.of(hostField, tagField)));
      logStore.logStore.commit();
      logStore.logStore.refresh();

      Collection<LogMessage> results =
          findAllMessages(logStore.logSearcher, MessageUtil.TEST_DATASET_NAME, "tag:foo-bar", 1000);
      assertThat(results.size()).isEqualTo(1);

      Collection<LogMessage> results2 =
          findAllMessages(
              logStore.logSearcher,
              MessageUtil.TEST_DATASET_NAME,
              "hostname:host1-dc2.abc.com",
              1000);
      assertThat(results2.size()).isEqualTo(1);

      Collection<LogMessage> results3 =
          findAllMessages(
              logStore.logSearcher, MessageUtil.TEST_DATASET_NAME, "hostname:xyz", 1000);
      assertThat(results3.size()).isEqualTo(0);

      Collection<LogMessage> results4 =
          findAllMessages(
              logStore.logSearcher, MessageUtil.TEST_DATASET_NAME, "hostname:host2", 1000);
      assertThat(results4.size()).isEqualTo(0);

      Collection<LogMessage> results5 =
          findAllMessages(
              logStore.logSearcher, MessageUtil.TEST_DATASET_NAME, "hostname:abc", 1000);
      assertThat(results5.size()).isEqualTo(0);

      Collection<LogMessage> results8 =
          findAllMessages(
              logStore.logSearcher, MessageUtil.TEST_DATASET_NAME, "hostname:abc.com", 1000);
      assertThat(results8.size()).isEqualTo(0);

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, logStore.metricsRegistry)).isEqualTo(1);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, logStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, logStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, logStore.metricsRegistry)).isEqualTo(1);
    }
  }

  @Nested
  public class SuppressExceptionsOnClosedWriter {
    @RegisterExtension
    public TemporaryLogStoreAndSearcherExtension testLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    public SuppressExceptionsOnClosedWriter() throws IOException {}

    @Test
    public void testSearcherOnclosedWriter() {
      addMessages(testLogStore.logStore, 1, 100, true);
      testLogStore.logStore.close();
      testLogStore.logStore = null;
      Collection<LogMessage> results =
          findAllMessages(testLogStore.logSearcher, MessageUtil.TEST_DATASET_NAME, "Message1", 100);
      assertThat(results.size()).isEqualTo(1);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, testLogStore.metricsRegistry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, testLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, testLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, testLogStore.metricsRegistry)).isEqualTo(1);
    }
  }

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder().silent().withSecureConnection(false).build();

  @Nested
  public class SnapshotTester {
    @RegisterExtension
    public TemporaryLogStoreAndSearcherExtension strictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    @TempDir private Path tmpPath;

    public SnapshotTester() throws IOException {}

    @Test
    public void testS3SnapshotWithPrefix() throws Exception {
      testS3Snapshot("test-bucket-with-prefix", "snapshot_prefix1");
    }

    @Test
    public void testS3SnapshotWithEmptyPrefix() throws Exception {
      testS3Snapshot("test-bucket-no-prefix", "");
    }

    private void testS3Snapshot(String bucket, String prefix) throws Exception {
      LuceneIndexStoreImpl logStore = strictLogStore.logStore;
      addMessages(logStore, 1, 100, true);
      Collection<LogMessage> results =
          findAllMessages(
              strictLogStore.logSearcher, MessageUtil.TEST_DATASET_NAME, "Message1", 100);
      assertThat(results.size()).isEqualTo(1);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry))
          .isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

      Path dirPath = logStore.getDirectory().getDirectory().toAbsolutePath();
      IndexCommit indexCommit = logStore.getIndexCommit();
      Collection<String> activeFiles = indexCommit.getFileNames();
      LocalBlobFs localBlobFs = new LocalBlobFs();

      logStore.close();
      strictLogStore.logSearcher.close();
      strictLogStore.logStore = null;
      strictLogStore.logSearcher = null;
      assertThat(localBlobFs.listFiles(dirPath.toUri(), false).length)
          .isGreaterThanOrEqualTo(activeFiles.size());

      // create an S3 client
      S3AsyncClient s3AsyncClient =
          S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
      S3CrtBlobFs s3CrtBlobFs = new S3CrtBlobFs(s3AsyncClient);
      s3AsyncClient.createBucket(CreateBucketRequest.builder().bucket(bucket).build()).get();

      // Copy files to S3.
      copyToS3(dirPath, activeFiles, bucket, prefix, s3CrtBlobFs);

      for (String fileName : activeFiles) {
        File fileToCopy = new File(dirPath.toString(), fileName);
        HeadObjectResponse headObjectResponse =
            s3AsyncClient
                .headObject(
                    S3TestUtils.getHeadObjectRequest(
                        bucket,
                        prefix != null && !prefix.isEmpty()
                            ? prefix + DELIMITER + fileName
                            : fileName))
                .get();
        assertThat(headObjectResponse.contentLength()).isEqualTo(fileToCopy.length());
      }

      // Download files from S3 to local FS.
      String[] s3Files = copyFromS3(bucket, prefix, s3CrtBlobFs, tmpPath.toAbsolutePath());
      assertThat(s3Files.length).isEqualTo(activeFiles.size());

      // Search files in local FS.
      LogIndexSearcherImpl newSearcher =
          new LogIndexSearcherImpl(
              LogIndexSearcherImpl.searcherManagerFromPath(tmpPath.toAbsolutePath()),
              logStore.getSchema());
      Collection<LogMessage> newResults =
          findAllMessages(newSearcher, MessageUtil.TEST_DATASET_NAME, "Message1", 100);
      assertThat(newResults.size()).isEqualTo(1);

      // Clean up
      logStore.releaseIndexCommit(indexCommit);
      newSearcher.close();
      s3CrtBlobFs.close();
    }

    @Test
    public void testLocalSnapshot() throws IOException {
      LuceneIndexStoreImpl logStore = strictLogStore.logStore;
      addMessages(logStore, 1, 100, true);
      Collection<LogMessage> results =
          findAllMessages(
              strictLogStore.logSearcher, MessageUtil.TEST_DATASET_NAME, "Message1", 100);
      assertThat(results.size()).isEqualTo(1);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry))
          .isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
      assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

      Path dirPath = logStore.getDirectory().getDirectory().toAbsolutePath();
      IndexCommit indexCommit = logStore.getIndexCommit();
      Collection<String> activeFiles = indexCommit.getFileNames();
      LocalBlobFs blobFs = new LocalBlobFs();
      logStore.close();
      strictLogStore.logSearcher.close();
      strictLogStore.logStore = null;
      strictLogStore.logSearcher = null;

      assertThat(blobFs.listFiles(dirPath.toUri(), false).length)
          .isGreaterThanOrEqualTo(activeFiles.size());

      copyToLocalPath(dirPath, activeFiles, tmpPath.toAbsolutePath(), blobFs);

      LogIndexSearcherImpl newSearcher =
          new LogIndexSearcherImpl(
              LogIndexSearcherImpl.searcherManagerFromPath(tmpPath.toAbsolutePath()),
              logStore.getSchema());

      Collection<LogMessage> newResults =
          findAllMessages(newSearcher, MessageUtil.TEST_DATASET_NAME, "Message1", 100);
      assertThat(newResults.size()).isEqualTo(1);
      logStore.releaseIndexCommit(indexCommit);
      newSearcher.close();
    }
  }

  @Nested
  public class IndexCleanupTests {
    @RegisterExtension
    public TemporaryLogStoreAndSearcherExtension strictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    public IndexCleanupTests() throws IOException {}

    @Test
    public void testCleanup() throws IOException {
      addMessages(strictLogStore.logStore, 1, 100, true);
      Collection<LogMessage> results =
          findAllMessages(
              strictLogStore.logSearcher, MessageUtil.TEST_DATASET_NAME, "Message1", 100);
      assertThat(results.size()).isEqualTo(1);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry))
          .isEqualTo(100);

      strictLogStore.logStore.close();
      strictLogStore.logSearcher.close();

      File tempFolder = strictLogStore.logStore.getDirectory().getDirectory().toFile();
      assertThat(tempFolder.exists()).isTrue();
      strictLogStore.logStore.cleanup();
      assertThat(tempFolder.exists()).isFalse();
      // Set the values to null so we don't do double cleanup.
      strictLogStore.logStore = null;
      strictLogStore.logSearcher = null;
    }
  }

  @Nested
  public class AutoCommitTests {
    Duration commitDuration = Duration.ofSeconds(5);

    @RegisterExtension
    public TemporaryLogStoreAndSearcherExtension testLogStore =
        new TemporaryLogStoreAndSearcherExtension(
            commitDuration,
            commitDuration,
            true,
            SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy
                .CONVERT_VALUE_AND_DUPLICATE_FIELD);

    public AutoCommitTests() throws IOException {}

    @Test
    public void testCommit() {
      addMessages(testLogStore.logStore, 1, 100, false);
      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, testLogStore.metricsRegistry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, testLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, testLogStore.metricsRegistry)).isEqualTo(0);
      assertThat(getTimerCount(COMMITS_TIMER, testLogStore.metricsRegistry)).isEqualTo(0);

      await()
          .until(
              () ->
                  findAllMessages(
                              testLogStore.logSearcher,
                              MessageUtil.TEST_DATASET_NAME,
                              "Message1",
                              10)
                          .size()
                      == 1);

      assertThat(getCount(MESSAGES_RECEIVED_COUNTER, testLogStore.metricsRegistry)).isEqualTo(100);
      assertThat(getCount(MESSAGES_FAILED_COUNTER, testLogStore.metricsRegistry)).isEqualTo(0);

      await()
          .until(
              () -> getTimerCount(REFRESHES_TIMER, testLogStore.metricsRegistry),
              (value) -> value >= 1 && value <= 3);
      await()
          .until(
              () -> getTimerCount(COMMITS_TIMER, testLogStore.metricsRegistry),
              (value) -> value >= 1 && value <= 3);
    }
  }

  @Test
  public void testMaxRamBufferCalculations() {
    assertThat(LuceneIndexStoreImpl.getRAMBufferSizeMB((long) 8e+9)).isEqualTo(800);
    assertThat(LuceneIndexStoreImpl.getRAMBufferSizeMB(Long.MAX_VALUE)).isEqualTo(256);
    assertThat(LuceneIndexStoreImpl.getRAMBufferSizeMB((long) 24e+9)).isEqualTo(2048);
  }
}
