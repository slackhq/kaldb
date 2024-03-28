package com.slack.kaldb.testlib;

import static com.slack.kaldb.testlib.MessageUtil.DEFAULT_MESSAGE_PREFIX;
import static com.slack.kaldb.testlib.MessageUtil.TEST_DATASET_NAME;
import static com.slack.kaldb.testlib.MessageUtil.TEST_SOURCE_DOUBLE_PROPERTY;
import static com.slack.kaldb.testlib.MessageUtil.TEST_SOURCE_FLOAT_PROPERTY;
import static com.slack.kaldb.testlib.MessageUtil.TEST_SOURCE_INT_PROPERTY;
import static com.slack.kaldb.testlib.MessageUtil.TEST_SOURCE_LONG_PROPERTY;
import static com.slack.kaldb.testlib.MessageUtil.TEST_SOURCE_STRING_PROPERTY;

import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.proto.schema.Schema;
import com.slack.service.murron.trace.Trace;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SpanUtil {

  public static final String BINARY_TAG_VALUE = "binaryTagValue";

  public static Trace.Span makeSpan(
      String traceId,
      String id,
      String parentId,
      long timestampMicros,
      long durationMicros,
      String name,
      String serviceName,
      String msgType) {
    Trace.Span.Builder spanBuilder =
        makeSpanBuilder(
            traceId, id, parentId, timestampMicros, durationMicros, name, serviceName, msgType);
    return spanBuilder.build();
  }

  private static Trace.Span.Builder makeSpanBuilder(
      String traceId,
      String id,
      String parentId,
      long timestampMicros,
      long durationMicros,
      String name,
      String serviceName,
      String msgType) {
    Trace.Span.Builder spanBuilder = Trace.Span.newBuilder();

    if (!id.isEmpty()) {
      spanBuilder.setId(ByteString.copyFrom(id.getBytes()));
    }
    if (!traceId.isEmpty()) {
      spanBuilder.setTraceId(ByteString.copyFrom(traceId.getBytes()));
    }
    if (!parentId.isEmpty()) {
      spanBuilder.setParentId(ByteString.copyFrom(parentId.getBytes()));
    }
    if (!name.isEmpty()) {
      spanBuilder.setName(name);
    }
    spanBuilder.setTimestamp(timestampMicros);
    spanBuilder.setDuration(durationMicros);

    List<Trace.KeyValue> tags = new ArrayList<>();
    // Set service tag
    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.SERVICE_NAME.fieldName)
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .setVStr(serviceName)
            .build());

    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey("http_method")
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .setVStr("POST")
            .build());

    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey("method")
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .setVStr("callbacks.flannel")
            .build());

    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey("boolean")
            .setFieldType(Schema.SchemaFieldType.BOOLEAN)
            .setVBool(true)
            .build());

    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey("int")
            .setFieldType(Schema.SchemaFieldType.LONG)
            .setVInt64(1000)
            .build());

    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey("float")
            .setFieldType(Schema.SchemaFieldType.DOUBLE)
            .setVFloat64(1001.2)
            .build());

    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey("binary")
            .setVBinary(ByteString.copyFromUtf8(BINARY_TAG_VALUE))
            .setFieldType(Schema.SchemaFieldType.BINARY)
            .setVStr("ignored")
            .build());

    tags.add(
        Trace.KeyValue.newBuilder()
            .setKey(LogMessage.ReservedField.TYPE.fieldName)
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .setVStr(msgType)
            .build());

    spanBuilder.addAllTags(tags);
    return spanBuilder;
  }

  public static Trace.Span makeSpan(int i) {
    return makeSpan(i, Instant.now());
  }

  public static Trace.Span makeSpan(int i, Instant timestamp) {
    String message =
        String.format("The identifier in this message is %s", DEFAULT_MESSAGE_PREFIX + i);
    return makeSpan(i, message, timestamp);
  }

  public static Trace.Span makeSpan(int i, String message, Instant timestamp) {
    return makeSpan(i, message, timestamp, List.of());
  }

  public static Trace.Span makeSpan(
      int i, String message, Instant timestamp, List<Trace.KeyValue> additionalTags) {
    String id = DEFAULT_MESSAGE_PREFIX + i;
    Trace.Span span =
        Trace.Span.newBuilder()
            .setTimestamp(
                TimeUnit.MICROSECONDS.convert(timestamp.toEpochMilli(), TimeUnit.MILLISECONDS))
            .setId(ByteString.copyFromUtf8(id))
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setVStr(message)
                    .setKey("message")
                    .setFieldType(Schema.SchemaFieldType.KEYWORD)
                    .build())
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setVInt32(i)
                    .setKey(TEST_SOURCE_INT_PROPERTY)
                    .setFieldType(Schema.SchemaFieldType.INTEGER)
                    .build())
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setVInt64(i)
                    .setKey(TEST_SOURCE_LONG_PROPERTY)
                    .setFieldType(Schema.SchemaFieldType.LONG)
                    .build())
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setVFloat32(i)
                    .setKey(TEST_SOURCE_FLOAT_PROPERTY)
                    .setFieldType(Schema.SchemaFieldType.FLOAT)
                    .build())
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setVFloat64(i)
                    .setKey(TEST_SOURCE_DOUBLE_PROPERTY)
                    .setFieldType(Schema.SchemaFieldType.DOUBLE)
                    .build())
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setVStr(String.format("String-%s", i))
                    .setKey(TEST_SOURCE_STRING_PROPERTY)
                    .setFieldType(Schema.SchemaFieldType.KEYWORD)
                    .build())
            .addTags(
                Trace.KeyValue.newBuilder()
                    .setVStr(TEST_DATASET_NAME)
                    .setKey(LogMessage.ReservedField.SERVICE_NAME.fieldName)
                    .setFieldType(Schema.SchemaFieldType.KEYWORD)
                    .build())
            .build();
    for (Trace.KeyValue additionalTag : additionalTags) {
      span = span.toBuilder().addTags(additionalTag).build();
    }
    return span;
  }

  public static List<Trace.Span> makeSpansWithTimeDifference(
      int low, int high, long timeDeltaMills, Instant start) {
    //    return List.of(IntStream.rangeClosed(0, high -low).mapToObj(i -> makeSpan(low+i,
    // start.plusNanos(1000 * 1000 * timeDeltaMills * i), List.of())).collect(Collectors.toList());

    List<Trace.Span> result = new ArrayList<>();
    for (int i = 0; i <= (high - low); i++) {
      result.add(makeSpan(low + i, start.plusNanos(1000 * 1000 * timeDeltaMills * i)));
    }
    return result;
  }
}
