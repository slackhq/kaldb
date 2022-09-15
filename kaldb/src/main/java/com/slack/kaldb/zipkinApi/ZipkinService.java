package com.slack.kaldb.zipkinApi;

import static com.slack.kaldb.logstore.LogMessage.ReservedField.TRACE_ID;
import static com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata.MATCH_ALL_DATASET;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.*;
import com.slack.kaldb.logstore.LogMessage.ReservedField;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.server.KaldbQueryServiceBase;
import com.slack.kaldb.util.JsonUtil;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
/**
 * Zipkin compatible API service
 *
 * @see <a
 *     href="https://github.com/grafana/grafana/blob/main/public/app/plugins/datasource/zipkin/datasource.ts">Grafana
 *     Zipkin API</a> <a
 *     href="https://github.com/openzipkin/zipkin-api/blob/master/zipkin.proto">Trace proto
 *     compatible with Zipkin</a> <a href="https://zipkin.io/zipkin-api/#/">Trace API Swagger
 *     Hub</a>
 */
public class ZipkinService {

  // intentionally returning LogWireMessage instead of LogMessage
  // If we return LogMessage the caller then needs to call getSource which is a deep copy of the
  // object
  private static List<LogWireMessage> searchResultToLogWireMessage(
          KaldbSearch.SearchResult searchResult) throws IOException {
    List<ByteString> hitsByteList = searchResult.getHitsList().asByteStringList();
    List<LogWireMessage> messages = new ArrayList<>(hitsByteList.size());
    for (ByteString byteString : hitsByteList) {
      LogWireMessage hit = JsonUtil.read(byteString.toStringUtf8(), LogWireMessage.class);
      // LogMessage message = LogMessage.fromWireMessage(hit);
      messages.add(hit);
    }
    return messages;
  }

  private static final Logger LOG = LoggerFactory.getLogger(ZipkinService.class);
  private static long LOOKBACK_MINS = 60 * 24;

  @VisibleForTesting public static int MAX_SPANS = 20_000;

  private final KaldbQueryServiceBase searcher;
  private static final JsonFormat.Printer printer =
      JsonFormat.printer().includingDefaultValueFields();

  public ZipkinService(KaldbQueryServiceBase searcher) {
    this.searcher = searcher;
  }

  @Get
  @Path("/api/v2/services")
  public HttpResponse getServices() throws IOException {
    String output = "[]";
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  @Get("/api/v2/spans")
  public HttpResponse getSpans(@Param("serviceName") Optional<String> serviceName)
      throws IOException {
    String output = "[]";
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  @Get("/api/v2/traces")
  public HttpResponse getTraces(
      @Param("serviceName") Optional<String> serviceName,
      @Param("spanName") Optional<String> spanName,
      @Param("annotationQuery") Optional<String> annotationQuery,
      @Param("minDuration") Optional<Integer> minDuration,
      @Param("maxDuration") Optional<Integer> maxDuration,
      @Param("endTs") Long endTs,
      @Param("lookback") Long lookback,
      @Param("limit") @Default("10") Integer limit)
      throws IOException {
    String output = "[]";
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  @Get("/api/v2/trace/{traceId}")
  public HttpResponse getTraceByTraceId(
      @Param("traceId") String traceId,
      @Param("startTimeEpochMs") Optional<Long> startTimeEpochMs,
      @Param("endTimeEpochMs") Optional<Long> endTimeEpochMs)
      throws IOException {

    String queryString = "trace_id:" + traceId;

    long startTime = Instant.now().minus(LOOKBACK_MINS, ChronoUnit.MINUTES).toEpochMilli();
    if (startTimeEpochMs.isPresent()) {
      startTime = startTimeEpochMs.get();
    }
    long endTime = System.currentTimeMillis();
    if (endTimeEpochMs.isPresent()) {
      endTime = endTimeEpochMs.get();
    }

    // TODO: when MAX_SPANS is hit the results will look weird because the index is sorted in
    // reverse timestamp and the spans returned will be the tail. We should support sort in the
    // search request
    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();
    KaldbSearch.SearchResult searchResult =
        searcher.doSearch(
            searchRequestBuilder
                .setDataset(MATCH_ALL_DATASET)
                .setQueryString(queryString)
                .setStartTimeEpochMs(startTime)
                .setEndTimeEpochMs(endTime)
                .setHowMany(MAX_SPANS)
                .setBucketCount(0)
                .build());
    List<LogWireMessage> messages = searchResultToLogWireMessage(searchResult);
    String output = convertLogWireMessageToZipkinSpan(messages);

    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  public static String convertLogWireMessageToZipkinSpan(List<LogWireMessage> messages)
      throws InvalidProtocolBufferException {
    List<String> traces = new ArrayList<>();
    int numSpans = 0;
    for (LogWireMessage message : messages) {
      if (numSpans++ >= MAX_SPANS) {
        break;
      }
      if (message.id == null) {
        LOG.warn("Document cannot have missing id");
        continue;
      }

      String messageTraceId = null;
      String parentId = null;
      String name = null;
      String serviceName = null;
      String timestamp = null;
      long duration = Long.MIN_VALUE;
      Map<String, String> messageTags = new HashMap<>();

      for (String k : message.source.keySet()) {
        Object value = message.source.get(k);

        if (ReservedField.isReservedField(k)) {
          switch (ReservedField.get(k)) {
            case TRACE_ID:
              messageTraceId = (String) value;
              break;
            case PARENT_ID:
              parentId = (String) value;
              break;
            case NAME:
              name = (String) value;
              break;
            case SERVICE_NAME:
              serviceName = (String) value;
              break;
            case TIMESTAMP:
              timestamp = (String) value;
              break;
            case DURATION_MS:
              duration = ((Number) value).longValue();
              break;
            default:
              messageTags.put(k, String.valueOf(value));
          }
        } else {
          messageTags.put(k, String.valueOf(value));
        }
      }
      // these are some mandatory fields without which the grafana zipkin plugin fails to display
      // the span
      if (messageTraceId == null) {
        messageTraceId = message.id;
      }
      if (timestamp == null) {
        LOG.warn("Document id={} missing @timestamp", message.id);
        continue;
      }

      final long messageConvertedTimestamp = convertToMicroSeconds(Instant.parse(timestamp));

      final Trace.ZipkinSpan span =
          makeSpan(
              messageTraceId,
              Optional.ofNullable(parentId),
              message.id,
              Optional.ofNullable(name),
              Optional.ofNullable(serviceName),
              messageConvertedTimestamp,
              duration,
              messageTags);
      String spanJson = printer.print(span);
      traces.add(spanJson);
    }
    StringJoiner outputJsonArray = new StringJoiner(",", "[", "]");
    traces.forEach(outputJsonArray::add);
    return outputJsonArray.toString();
  }

  @VisibleForTesting
  // Epoch microseconds of the start of this span, possibly absent if this an incomplete span
  public static long convertToMicroSeconds(Instant instant) {
    return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
  }

  // We sadly need to create ZipkinSpan and can't reuse the Span proto
  // The reason being, Span has id/traceId/parentId as bytes (which is correct and is how the proto
  // definition is defined - there shouldn't be a string without encoding)
  // However if we ship back the bytes ( encoded data ) back to grafana it has no idea that the data
  // needs to decoded. Hence we decode it back and ship a ZipkinSpan
  public static Trace.ZipkinSpan makeSpan(
      String traceId,
      Optional<String> parentId,
      String id,
      Optional<String> name,
      Optional<String> serviceName,
      long timestamp,
      long duration,
      Map<String, String> tags) {
    Trace.ZipkinSpan.Builder spanBuilder = Trace.ZipkinSpan.newBuilder();

    spanBuilder.setTraceId(ByteString.copyFrom(traceId.getBytes()).toStringUtf8());
    spanBuilder.setId(ByteString.copyFrom(id.getBytes()).toStringUtf8());
    spanBuilder.setTimestamp(timestamp);
    spanBuilder.setDuration(duration);

    parentId.ifPresent(
        s -> spanBuilder.setParentId(ByteString.copyFrom(s.getBytes()).toStringUtf8()));
    name.ifPresent(spanBuilder::setName);
    serviceName.ifPresent(
        s -> spanBuilder.setRemoteEndpoint(Trace.Endpoint.newBuilder().setServiceName(s)));
    spanBuilder.putAllTags(tags);
    return spanBuilder.build();
  }
}
