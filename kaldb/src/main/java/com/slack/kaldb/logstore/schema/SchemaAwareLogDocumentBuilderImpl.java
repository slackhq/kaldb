package com.slack.kaldb.logstore.schema;

import static com.slack.kaldb.logstore.LogMessage.computedIndexName;
import static com.slack.kaldb.logstore.schema.SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_VALUE_AND_DUPLICATE_FIELD;
import static com.slack.kaldb.writer.SpanFormatter.DEFAULT_INDEX_NAME;
import static com.slack.kaldb.writer.SpanFormatter.DEFAULT_LOG_MESSAGE_TYPE;
import static com.slack.kaldb.writer.SpanFormatter.isValidTimestamp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.slack.kaldb.logstore.DocumentBuilder;
import com.slack.kaldb.logstore.FieldDefMismatchException;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.metadata.schema.FieldType;
import com.slack.kaldb.metadata.schema.LuceneFieldDef;
import com.slack.kaldb.proto.schema.Schema;
import com.slack.kaldb.util.JsonUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.util.Strings;
import org.apache.lucene.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SchemaAwareLogDocumentBuilder always indexes a field using the same type. It doesn't allow field
 * conflicts.
 *
 * <p>In case of a field conflict, this class uses FieldConflictPolicy to handle them.
 *
 * <p>NOTE: Currently, if building a document raises errors, we still register the type of the
 * fields in this document partially. While the document may not be indexed, this partial field
 * config will exist in the system. For now, we assume storing this metadata is fine since it is
 * rarely an issue and helps with performance. If this is an issue, we need to scan the json twice
 * to ensure document is good to index.
 */
public class SchemaAwareLogDocumentBuilderImpl implements DocumentBuilder {
  private static final Logger LOG =
      LoggerFactory.getLogger(SchemaAwareLogDocumentBuilderImpl.class);

  // TODO: In future, make this value configurable.
  private static final int MAX_NESTING_DEPTH = 3;

  private static void addTextField(
      ImmutableMap.Builder<String, LuceneFieldDef> fieldDefBuilder,
      String fieldName,
      boolean isStored,
      boolean isIndexed) {
    fieldDefBuilder.put(
        fieldName, new LuceneFieldDef(fieldName, FieldType.TEXT.name, isStored, isIndexed, false));
  }

  // TODO: Move this definition to the config file.
  public static ImmutableMap<String, LuceneFieldDef> getDefaultLuceneFieldDefinitions(
      boolean enableFullTextSearch) {
    ImmutableMap.Builder<String, LuceneFieldDef> fieldDefBuilder = ImmutableMap.builder();

    addTextField(fieldDefBuilder, LogMessage.SystemField.SOURCE.fieldName, true, false);
    addTextField(fieldDefBuilder, LogMessage.ReservedField.MESSAGE.fieldName, false, true);
    if (enableFullTextSearch) {
      addTextField(fieldDefBuilder, LogMessage.SystemField.ALL.fieldName, false, true);
    }

    String[] fieldsAsString = {
      LogMessage.SystemField.INDEX.fieldName,
      LogMessage.ReservedField.TYPE.fieldName,
      LogMessage.ReservedField.HOSTNAME.fieldName,
      LogMessage.ReservedField.PACKAGE.fieldName,
      LogMessage.ReservedField.TAG.fieldName,
      LogMessage.ReservedField.USERNAME.fieldName,
      LogMessage.ReservedField.PAYLOAD.fieldName,
      LogMessage.ReservedField.NAME.fieldName,
      LogMessage.ReservedField.SERVICE_NAME.fieldName,
      LogMessage.ReservedField.TRACE_ID.fieldName,
      LogMessage.ReservedField.PARENT_ID.fieldName
    };

    for (String fieldName : fieldsAsString) {
      fieldDefBuilder.put(
          fieldName, new LuceneFieldDef(fieldName, FieldType.STRING.name, false, true, true));
    }

    String[] fieldsAsIds = {
      LogMessage.SystemField.ID.fieldName,
    };
    for (String fieldName : fieldsAsIds) {
      fieldDefBuilder.put(
          fieldName, new LuceneFieldDef(fieldName, FieldType.ID.name, false, true, true));
    }

    String[] fieldsAsLong = {
      LogMessage.ReservedField.DURATION_MS.fieldName,
      LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
    };

    for (String fieldName : fieldsAsLong) {
      fieldDefBuilder.put(
          fieldName, new LuceneFieldDef(fieldName, FieldType.LONG.name, false, true, true));
    }

    return fieldDefBuilder.build();
  }

  /**
   * This enum tracks the field conflict policy for a chunk.
   *
   * <p>NOTE: In future, we may need this granularity at a per field level. Also, other potential
   * options for handling these conflicts: (a) store all the conflicted fields as strings by default
   * so querying those fields is more consistent. (b) duplicate field value only but don't create a
   * field.
   */
  public enum FieldConflictPolicy {
    // Throw an error on field conflict.
    RAISE_ERROR,
    // Drop the conflicting field.
    DROP_FIELD,
    // Convert the field value to the type of the conflicting field.
    CONVERT_FIELD_VALUE,
    // Convert the field value to the type of conflicting field and also create a new field of type.
    CONVERT_VALUE_AND_DUPLICATE_FIELD
  }

  private void addField(
      final Document doc,
      final String key,
      final Object value,
      final Schema.SchemaFieldType schemaFieldType,
      final String keyPrefix,
      int nestingDepth) {
    // If value is a list, convert the value to a String and index the field.
    if (value instanceof List) {
      addField(doc, key, Strings.join((List) value, ','), schemaFieldType, keyPrefix, nestingDepth);
      return;
    }

    String fieldName = keyPrefix.isBlank() || keyPrefix.isEmpty() ? key : keyPrefix + "." + key;
    // Ingest nested map field recursively upto max nesting. After that index it as a string.
    if (value instanceof Map) {
      if (nestingDepth >= MAX_NESTING_DEPTH) {
        // Once max nesting depth is reached, index the field as a string.
        addField(doc, key, value.toString(), schemaFieldType, keyPrefix, nestingDepth + 1);
      } else {
        Map<Object, Object> mapValue = (Map<Object, Object>) value;
        for (Object k : mapValue.keySet()) {
          if (k instanceof String) {
            addField(
                doc, (String) k, mapValue.get(k), schemaFieldType, fieldName, nestingDepth + 1);
          } else {
            throw new FieldDefMismatchException(
                String.format(
                    "Field %s, %s has an non-string type which is unsupported", k, value));
          }
        }
      }
      return;
    }

    FieldType valueType = FieldType.valueOf(schemaFieldType.name());
    if (!fieldDefMap.containsKey(fieldName)) {
      indexNewField(doc, fieldName, value, schemaFieldType);
    } else {
      LuceneFieldDef registeredField = fieldDefMap.get(fieldName);
      // If the field types are same or the fields are type aliases
      if (registeredField.fieldType == valueType
          || FieldType.areTypeAliasedFieldTypes(registeredField.fieldType, valueType)) {
        // No field conflicts index it using previous description.
        // Pass in registeredField here since the valueType and registeredField may be aliases
        indexTypedField(doc, fieldName, value, registeredField);
      } else {
        // There is a field type conflict, index it using the field conflict policy.
        switch (indexFieldConflictPolicy) {
          case DROP_FIELD:
            LOG.debug("Dropped field {} due to field type conflict", fieldName);
            droppedFieldsCounter.increment();
            break;
          case CONVERT_FIELD_VALUE:
            convertValueAndIndexField(value, valueType, registeredField, doc, fieldName);
            LOG.debug(
                "Converting field {} value from type {} to {} due to type conflict",
                fieldName,
                valueType,
                registeredField.fieldType);
            convertFieldValueCounter.increment();
            break;
          case CONVERT_VALUE_AND_DUPLICATE_FIELD:
            convertValueAndIndexField(value, valueType, registeredField, doc, fieldName);
            LOG.debug(
                "Converting field {} value from type {} to {} due to type conflict",
                fieldName,
                valueType,
                registeredField.fieldType);
            // Add new field with new type
            String newFieldName = makeNewFieldOfType(fieldName, valueType);
            indexNewField(doc, newFieldName, value, schemaFieldType);
            LOG.debug(
                "Added new field {} of type {} due to type conflict", newFieldName, valueType);
            convertAndDuplicateFieldCounter.increment();
            break;
          case RAISE_ERROR:
            throw new FieldDefMismatchException(
                String.format(
                    "Field type for field %s is %s but new value is of type  %s. ",
                    fieldName, registeredField.fieldType, valueType));
        }
      }
    }
  }

  private void indexNewField(
      Document doc, String key, Object value, Schema.SchemaFieldType schemaFieldType) {
    final LuceneFieldDef newFieldDef = getLuceneFieldDef(key, schemaFieldType);
    totalFieldsCounter.increment();
    fieldDefMap.put(key, newFieldDef);
    indexTypedField(doc, key, value, newFieldDef);
  }

  private boolean isStored(String fieldName) {
    return fieldName.equals(LogMessage.SystemField.SOURCE.fieldName);
  }

  private boolean isDocValueField(Schema.SchemaFieldType schemaFieldType, String fieldName) {
    return !fieldName.equals(LogMessage.SystemField.SOURCE.fieldName)
        || !schemaFieldType.equals(Schema.SchemaFieldType.TEXT);
  }

  private boolean isIndexed(Schema.SchemaFieldType schemaFieldType, String fieldName) {
    return !fieldName.equals(LogMessage.SystemField.SOURCE.fieldName)
        || !schemaFieldType.equals(Schema.SchemaFieldType.BINARY);
  }

  // In the future, we need this to take SchemaField instead of FieldType
  // that way we can make isIndexed/isStored etc. configurable
  // we don't put it in th proto today but when we move to ZK we'll change the KeyValue to take
  // SchemaField info in the future
  private LuceneFieldDef getLuceneFieldDef(String key, Schema.SchemaFieldType schemaFieldType) {
    return new LuceneFieldDef(
        key,
        schemaFieldType.name(),
        isStored(key),
        isIndexed(schemaFieldType, key),
        isDocValueField(schemaFieldType, key));
  }

  static String makeNewFieldOfType(String key, FieldType valueType) {
    return key + "_" + valueType.getName();
  }

  private void convertValueAndIndexField(
      Object value, FieldType valueType, LuceneFieldDef registeredField, Document doc, String key) {
    try {
      Object convertedValue =
          FieldType.convertFieldValue(value, valueType, registeredField.fieldType);
      if (convertedValue != null) {
        indexTypedField(doc, key, convertedValue, registeredField);
      } else {
        LOG.warn(
            "No mapping found to convert value from={} to={}",
            valueType.name,
            registeredField.fieldType.name);
        convertErrorCounter.increment();
      }
    } catch (Exception e) {
      LOG.warn(
          "Could not convert value={} from={} to={}",
          value.toString(),
          valueType.name,
          registeredField.fieldType.name);
      convertErrorCounter.increment();
    }
  }

  private static void indexTypedField(
      Document doc, String key, Object value, LuceneFieldDef fieldDef) {
    fieldDef.fieldType.addField(doc, key, value, fieldDef);
  }

  public static SchemaAwareLogDocumentBuilderImpl build(
      FieldConflictPolicy fieldConflictPolicy,
      boolean enableFullTextSearch,
      MeterRegistry meterRegistry) {
    // Add basic fields by default
    return new SchemaAwareLogDocumentBuilderImpl(
        fieldConflictPolicy,
        getDefaultLuceneFieldDefinitions(enableFullTextSearch),
        enableFullTextSearch,
        meterRegistry);
  }

  static final String DROP_FIELDS_COUNTER = "dropped_fields";
  static final String CONVERT_ERROR_COUNTER = "convert_errors";
  static final String CONVERT_FIELD_VALUE_COUNTER = "convert_field_value";
  static final String CONVERT_AND_DUPLICATE_FIELD_COUNTER = "convert_and_duplicate_field";
  public static final String TOTAL_FIELDS_COUNTER = "total_fields";

  private final FieldConflictPolicy indexFieldConflictPolicy;
  private final boolean enableFullTextSearch;
  private final ConcurrentHashMap<String, LuceneFieldDef> fieldDefMap = new ConcurrentHashMap<>();
  private final Counter droppedFieldsCounter;
  private final Counter convertErrorCounter;
  private final Counter convertFieldValueCounter;
  private final Counter convertAndDuplicateFieldCounter;
  private final Counter totalFieldsCounter;

  SchemaAwareLogDocumentBuilderImpl(
      FieldConflictPolicy indexFieldConflictPolicy,
      final Map<String, LuceneFieldDef> initialFields,
      boolean enableFullTextSearch,
      MeterRegistry meterRegistry) {
    this.indexFieldConflictPolicy = indexFieldConflictPolicy;
    this.enableFullTextSearch = enableFullTextSearch;
    // Note: Consider adding field name as a tag to help debugging, but it's high cardinality.
    droppedFieldsCounter = meterRegistry.counter(DROP_FIELDS_COUNTER);
    convertFieldValueCounter = meterRegistry.counter(CONVERT_FIELD_VALUE_COUNTER);
    convertAndDuplicateFieldCounter = meterRegistry.counter(CONVERT_AND_DUPLICATE_FIELD_COUNTER);
    convertErrorCounter = meterRegistry.counter(CONVERT_ERROR_COUNTER);
    totalFieldsCounter = meterRegistry.counter(TOTAL_FIELDS_COUNTER);

    totalFieldsCounter.increment(initialFields.size());
    fieldDefMap.putAll(initialFields);
  }

  @Override
  public Document fromMessage(Trace.Span message) throws JsonProcessingException {
    Document doc = new Document();

    // today we rely on source to construct the document at search time so need to keep in
    // consistent for now
    Map<String, Object> jsonMap = new HashMap<>();
    if (!message.getParentId().isEmpty()) {
      jsonMap.put(
          LogMessage.ReservedField.PARENT_ID.fieldName, message.getParentId().toStringUtf8());
      addField(
          doc,
          LogMessage.ReservedField.PARENT_ID.fieldName,
          message.getParentId().toStringUtf8(),
          Schema.SchemaFieldType.KEYWORD,
          "",
          0);
    }
    if (!message.getTraceId().isEmpty()) {
      jsonMap.put(LogMessage.ReservedField.TRACE_ID.fieldName, message.getTraceId().toStringUtf8());
      addField(
          doc,
          LogMessage.ReservedField.TRACE_ID.fieldName,
          message.getTraceId().toStringUtf8(),
          Schema.SchemaFieldType.KEYWORD,
          "",
          0);
    }
    if (!message.getName().isEmpty()) {
      jsonMap.put(LogMessage.ReservedField.NAME.fieldName, message.getName());
      addField(
          doc,
          LogMessage.ReservedField.NAME.fieldName,
          message.getName(),
          Schema.SchemaFieldType.KEYWORD,
          "",
          0);
    }
    if (message.getDuration() != 0) {
      jsonMap.put(
          LogMessage.ReservedField.DURATION_MS.fieldName,
          Duration.of(message.getDuration(), ChronoUnit.MICROS).toMillis());
      addField(
          doc,
          LogMessage.ReservedField.DURATION_MS.fieldName,
          Duration.of(message.getDuration(), ChronoUnit.MICROS).toMillis(),
          Schema.SchemaFieldType.LONG,
          "",
          0);
    }
    if (!message.getId().isEmpty()) {
      addField(
          doc,
          LogMessage.SystemField.ID.fieldName,
          message.getId().toStringUtf8(),
          Schema.SchemaFieldType.KEYWORD,
          "",
          0);
    } else {
      throw new IllegalArgumentException("Span id is empty");
    }

    Instant timestamp =
        Instant.ofEpochMilli(
            TimeUnit.MILLISECONDS.convert(message.getTimestamp(), TimeUnit.MICROSECONDS));
    if (!isValidTimestamp(timestamp)) {
      timestamp = Instant.now();
      addField(
          doc,
          LogMessage.ReservedField.KALDB_INVALID_TIMESTAMP.fieldName,
          message.getTimestamp(),
          Schema.SchemaFieldType.LONG,
          "",
          0);
      jsonMap.put(
          LogMessage.ReservedField.KALDB_INVALID_TIMESTAMP.fieldName, message.getTimestamp());
    }

    addField(
        doc,
        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
        timestamp.toEpochMilli(),
        Schema.SchemaFieldType.LONG,
        "",
        0);
    // todo - this should be removed once we simplify the time handling
    // this will be overridden below if a user provided value exists
    jsonMap.put(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, timestamp.toString());

    Map<String, Trace.KeyValue> tags =
        message.getTagsList().stream()
            .map(keyValue -> Map.entry(keyValue.getKey(), keyValue))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, Map.Entry::getValue, (firstKV, dupKV) -> firstKV));

    // This should just be top level Trace.Span fields. This is error prone - what if type is
    // not a string?
    // Also in BulkApiRequestParser we basically take the index field and put it as a tag. So we're
    // just doing more work on both sides
    String indexName =
        tags.containsKey(LogMessage.ReservedField.SERVICE_NAME.fieldName)
            ? tags.get(LogMessage.ReservedField.SERVICE_NAME.fieldName).getVStr()
            : DEFAULT_INDEX_NAME;
    // if we don't do this LogMessage#isValid will be unhappy when we recreate the message
    // we need to fix this!!!
    indexName = computedIndexName(indexName);
    String msgType =
        tags.containsKey(LogMessage.ReservedField.TYPE.fieldName)
            ? tags.get(LogMessage.ReservedField.TYPE.fieldName).getVStr()
            : DEFAULT_LOG_MESSAGE_TYPE;

    addField(
        doc,
        LogMessage.ReservedField.TYPE.fieldName,
        msgType,
        Schema.SchemaFieldType.KEYWORD,
        "",
        0);

    jsonMap.put(LogMessage.ReservedField.SERVICE_NAME.fieldName, indexName);
    addField(
        doc,
        LogMessage.SystemField.INDEX.fieldName,
        indexName,
        Schema.SchemaFieldType.KEYWORD,
        "",
        0);
    addField(
        doc,
        LogMessage.ReservedField.SERVICE_NAME.fieldName,
        indexName,
        Schema.SchemaFieldType.KEYWORD,
        "",
        0);

    tags.remove(LogMessage.ReservedField.SERVICE_NAME.fieldName);
    tags.remove(LogMessage.ReservedField.TYPE.fieldName);

    // if any top level fields are in the tags, we should remove them
    tags.remove(LogMessage.ReservedField.PARENT_ID.fieldName);
    tags.remove(LogMessage.ReservedField.TRACE_ID.fieldName);
    tags.remove(LogMessage.ReservedField.NAME.fieldName);
    tags.remove(LogMessage.ReservedField.DURATION_MS.fieldName);
    tags.remove(LogMessage.SystemField.ID.fieldName);

    for (Trace.KeyValue keyValue : tags.values()) {
      Schema.SchemaFieldType schemaFieldType = keyValue.getFieldType();
      // move to switch statements
      if (schemaFieldType == Schema.SchemaFieldType.STRING
          || schemaFieldType == Schema.SchemaFieldType.KEYWORD) {
        addField(doc, keyValue.getKey(), keyValue.getVStr(), Schema.SchemaFieldType.KEYWORD, "", 0);
        jsonMap.put(keyValue.getKey(), keyValue.getVStr());
      } else if (schemaFieldType == Schema.SchemaFieldType.TEXT) {
        addField(doc, keyValue.getKey(), keyValue.getVStr(), Schema.SchemaFieldType.TEXT, "", 0);
        jsonMap.put(keyValue.getKey(), keyValue.getVStr());
      } else if (schemaFieldType == Schema.SchemaFieldType.IP) {
        addField(doc, keyValue.getKey(), keyValue.getVStr(), Schema.SchemaFieldType.IP, "", 0);
        jsonMap.put(keyValue.getKey(), keyValue.getVStr());
      } else if (schemaFieldType == Schema.SchemaFieldType.DATE) {
        addField(doc, keyValue.getKey(), keyValue.getVInt64(), Schema.SchemaFieldType.DATE, "", 0);
        jsonMap.put(keyValue.getKey(), keyValue.getVInt64());
      } else if (schemaFieldType == Schema.SchemaFieldType.BOOLEAN) {
        addField(
            doc, keyValue.getKey(), keyValue.getVBool(), Schema.SchemaFieldType.BOOLEAN, "", 0);
        jsonMap.put(keyValue.getKey(), keyValue.getVBool());
      } else if (schemaFieldType == Schema.SchemaFieldType.DOUBLE) {
        addField(
            doc, keyValue.getKey(), keyValue.getVFloat64(), Schema.SchemaFieldType.DOUBLE, "", 0);
        jsonMap.put(keyValue.getKey(), keyValue.getVFloat64());
      } else if (schemaFieldType == Schema.SchemaFieldType.FLOAT) {
        addField(
            doc, keyValue.getKey(), keyValue.getVFloat32(), Schema.SchemaFieldType.FLOAT, "", 0);
        jsonMap.put(keyValue.getKey(), keyValue.getVFloat32());
      } else if (schemaFieldType == Schema.SchemaFieldType.HALF_FLOAT) {
        addField(
            doc,
            keyValue.getKey(),
            keyValue.getVFloat32(),
            Schema.SchemaFieldType.HALF_FLOAT,
            "",
            0);
        jsonMap.put(keyValue.getKey(), keyValue.getVFloat32());
      } else if (schemaFieldType == Schema.SchemaFieldType.INTEGER) {
        addField(
            doc, keyValue.getKey(), keyValue.getVInt32(), Schema.SchemaFieldType.INTEGER, "", 0);
        jsonMap.put(keyValue.getKey(), keyValue.getVInt32());
      } else if (schemaFieldType == Schema.SchemaFieldType.LONG) {
        addField(doc, keyValue.getKey(), keyValue.getVInt64(), Schema.SchemaFieldType.LONG, "", 0);
        jsonMap.put(keyValue.getKey(), keyValue.getVInt64());
      } else if (schemaFieldType == Schema.SchemaFieldType.SCALED_LONG) {
        addField(doc, keyValue.getKey(), keyValue.getVInt64(), Schema.SchemaFieldType.LONG, "", 0);
        jsonMap.put(keyValue.getKey(), keyValue.getVInt64());
      } else if (schemaFieldType == Schema.SchemaFieldType.SHORT) {
        addField(doc, keyValue.getKey(), keyValue.getVInt32(), Schema.SchemaFieldType.SHORT, "", 0);
        jsonMap.put(keyValue.getKey(), keyValue.getVInt32());
      } else if (schemaFieldType == Schema.SchemaFieldType.BYTE) {
        addField(doc, keyValue.getKey(), keyValue.getVInt32(), Schema.SchemaFieldType.BYTE, "", 0);
        jsonMap.put(keyValue.getKey(), keyValue.getVInt32());
      } else if (schemaFieldType == Schema.SchemaFieldType.BINARY) {
        addField(
            doc, keyValue.getKey(), keyValue.getVBinary(), Schema.SchemaFieldType.BINARY, "", 0);
        jsonMap.put(keyValue.getKey(), keyValue.getVBinary().toStringUtf8());
      } else {
        LOG.warn(
            "Skipping field with unknown field type {} with key {}",
            schemaFieldType,
            keyValue.getKey());
      }
    }

    LogWireMessage logWireMessage =
        new LogWireMessage(indexName, msgType, message.getId().toStringUtf8(), timestamp, jsonMap);
    final String msgString = JsonUtil.writeAsString(logWireMessage);
    addField(
        doc,
        LogMessage.SystemField.SOURCE.fieldName,
        msgString,
        Schema.SchemaFieldType.TEXT,
        "",
        0);
    if (enableFullTextSearch) {
      addField(
          doc, LogMessage.SystemField.ALL.fieldName, msgString, Schema.SchemaFieldType.TEXT, "", 0);
    }

    return doc;
  }

  @Override
  public ConcurrentHashMap<String, LuceneFieldDef> getSchema() {
    return fieldDefMap;
  }
}
