package com.slack.kaldb.logstore.schema;

import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_FIELD_VALUE;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_VALUE_AND_DUPLICATE_FIELD;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.DROP_FIELD;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.RAISE_ERROR;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.build;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.makeNewFieldOfType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.slack.kaldb.logstore.FieldDefMismatchException;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.metadata.schema.FieldType;
import com.slack.kaldb.proto.schema.Schema;
import com.slack.kaldb.testlib.SpanUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FieldConflictStrategyTests {

  private SimpleMeterRegistry meterRegistry;

  SchemaAwareLogDocumentBuilderImpl raiseErrorDocBuilder;
  SchemaAwareLogDocumentBuilderImpl dropFieldDocBuilder;
  SchemaAwareLogDocumentBuilderImpl convertFieldDocBuilder;
  SchemaAwareLogDocumentBuilderImpl convertAndDuplicateFieldDocBuilder;

  @BeforeEach
  public void setup() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    raiseErrorDocBuilder = build(RAISE_ERROR, true, meterRegistry);
    dropFieldDocBuilder = build(DROP_FIELD, true, meterRegistry);
    convertFieldDocBuilder = build(CONVERT_FIELD_VALUE, true, meterRegistry);
    convertAndDuplicateFieldDocBuilder =
        build(CONVERT_VALUE_AND_DUPLICATE_FIELD, true, meterRegistry);
  }

  @Test
  public void testFieldConflictNumericValues() throws JsonProcessingException {
    // index doc1 with "1" and doc2 with 1 and see how each strategy deals with it

    final String conflictingFieldName = "conflictingField";
    Trace.KeyValue hostField =
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.HOSTNAME.fieldName)
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .setVStr("host1-dc2.abc.com")
            .build();

    Trace.KeyValue tagField =
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.TAG.fieldName)
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .setVStr("foo-bar")
            .build();

    Trace.KeyValue conflictingTagStr =
        Trace.KeyValue.newBuilder()
            .setKey(conflictingFieldName)
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .setVStr("1")
            .build();

    Trace.KeyValue conflictingTagInt =
        Trace.KeyValue.newBuilder()
            .setKey(conflictingFieldName)
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .setVInt32(1)
            .build();

    Trace.Span doc1 =
        SpanUtil.makeSpan(
            1, "Test message", Instant.now(), List.of(hostField, tagField, conflictingTagStr));
    Trace.Span doc2 =
        SpanUtil.makeSpan(
            2, "Test message", Instant.now(), List.of(hostField, tagField, conflictingTagInt));

    Document msg1Doc = raiseErrorDocBuilder.fromMessage(doc1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(29);

    try {
      raiseErrorDocBuilder.fromMessage(doc2);
      fail("Expected FieldDefMismatchException");
    } catch (FieldDefMismatchException mismatchException) {
      // expected
    }

    msg1Doc = dropFieldDocBuilder.fromMessage(doc1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(29);

    Document msg2Doc = dropFieldDocBuilder.fromMessage(doc2);
    // 2 less because docValue is also missing
    assertThat(msg2Doc.getFields().size()).isEqualTo(27);

    msg1Doc = convertFieldDocBuilder.fromMessage(doc1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(29);

    msg2Doc = convertFieldDocBuilder.fromMessage(doc2);
    assertThat(msg2Doc.getFields().size()).isEqualTo(29);

    msg1Doc = convertAndDuplicateFieldDocBuilder.fromMessage(doc1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(29);

    msg2Doc = convertAndDuplicateFieldDocBuilder.fromMessage(doc2);
    assertThat(msg2Doc.getFields().size()).isEqualTo(31);
    String additionalCreatedFieldName = makeNewFieldOfType(conflictingFieldName, FieldType.INTEGER);
    // Value converted and new field is added.
    assertThat(getFieldCount(msg2Doc, Set.of(conflictingFieldName, additionalCreatedFieldName)))
        .isEqualTo(4);
  }

  @Test
  public void testFieldConflictBooleanValues() throws JsonProcessingException {
    // index doc1 with true and doc2 with "random" and doc3 as "1" (which should be interpreted as
    // true)

    final String conflictingFieldName = "conflictingField";
    Trace.KeyValue hostField =
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.HOSTNAME.fieldName)
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .setVStr("host1-dc2.abc.com")
            .build();

    Trace.KeyValue tagField =
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.TAG.fieldName)
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .setVStr("foo-bar")
            .build();

    Trace.KeyValue conflictingTagBool =
        Trace.KeyValue.newBuilder()
            .setKey(conflictingFieldName)
            .setFieldType(Schema.SchemaFieldType.BOOLEAN)
            .setVBool(true)
            .build();

    Trace.KeyValue conflictingTagStr =
        Trace.KeyValue.newBuilder()
            .setKey(conflictingFieldName)
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .setVStr("random")
            .build();

    Trace.KeyValue conflictingTagInt =
        Trace.KeyValue.newBuilder()
            .setKey(conflictingFieldName)
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .setVInt32(1)
            .build();

    Trace.Span doc1 =
        SpanUtil.makeSpan(
            1, "Test message", Instant.now(), List.of(hostField, tagField, conflictingTagBool));
    Trace.Span doc2 =
        SpanUtil.makeSpan(
            2, "Test message", Instant.now(), List.of(hostField, tagField, conflictingTagStr));

    Trace.Span doc3 =
        SpanUtil.makeSpan(
            3, "Test message", Instant.now(), List.of(hostField, tagField, conflictingTagInt));

    Document msg1Doc = raiseErrorDocBuilder.fromMessage(doc1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(29);

    try {
      raiseErrorDocBuilder.fromMessage(doc2);
      fail("Expected FieldDefMismatchException");
    } catch (FieldDefMismatchException mismatchException) {
      // expected
    }

    try {
      raiseErrorDocBuilder.fromMessage(doc3);
      fail("Expected FieldDefMismatchException");
    } catch (FieldDefMismatchException mismatchException) {
      // expected
    }

    msg1Doc = dropFieldDocBuilder.fromMessage(doc1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(29);

    Document msg2Doc = dropFieldDocBuilder.fromMessage(doc2);
    // 2 less because docValue is also missing
    assertThat(msg2Doc.getFields().size()).isEqualTo(27);

    Document msg3Doc = dropFieldDocBuilder.fromMessage(doc3);
    // 2 less because docValue is also missing
    assertThat(msg3Doc.getFields().size()).isEqualTo(27);

    msg1Doc = convertFieldDocBuilder.fromMessage(doc1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(29);

    msg2Doc = convertFieldDocBuilder.fromMessage(doc2);
    assertThat(msg2Doc.getFields().size()).isEqualTo(29);
    // If this ever fails is because message ordering changed the getField returns the DV variant
    assertThat(msg2Doc.getField(conflictingFieldName).binaryValue().utf8ToString()).isEqualTo("F");

    msg3Doc = convertFieldDocBuilder.fromMessage(doc3);
    assertThat(msg3Doc.getFields().size()).isEqualTo(29);
    assertThat(msg3Doc.getField(conflictingFieldName).binaryValue().utf8ToString()).isEqualTo("T");

    msg1Doc = convertAndDuplicateFieldDocBuilder.fromMessage(doc1);
    assertThat(msg1Doc.getFields().size()).isEqualTo(29);

    msg2Doc = convertAndDuplicateFieldDocBuilder.fromMessage(doc2);
    assertThat(msg2Doc.getFields().size()).isEqualTo(31);
    String additionalCreatedFieldName = makeNewFieldOfType(conflictingFieldName, FieldType.KEYWORD);
    // Value converted and new field is added.
    assertThat(getFieldCount(msg2Doc, Set.of(conflictingFieldName, additionalCreatedFieldName)))
        .isEqualTo(4);

    msg3Doc = convertAndDuplicateFieldDocBuilder.fromMessage(doc3);
    assertThat(msg3Doc.getFields().size()).isEqualTo(31);
    additionalCreatedFieldName = makeNewFieldOfType(conflictingFieldName, FieldType.INTEGER);
    // Value converted and new field is added.
    assertThat(getFieldCount(msg3Doc, Set.of(conflictingFieldName, additionalCreatedFieldName)))
        .isEqualTo(4);
  }

  public long getFieldCount(Document doc, Set<String> fieldNames) {
    return doc.getFields().stream().filter(f -> fieldNames.contains(f.name())).count();
  }
}
