package com.slack.kaldb.writer;

import static com.slack.kaldb.writer.MurronLogFormatter.API_LOG_DURATION_FIELD;
import static com.slack.kaldb.writer.MurronLogFormatter.ENVOY_DURATION_FIELD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.proto.schema.Schema;
import com.slack.service.murron.Murron;
import com.slack.service.murron.trace.Trace;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

public class MurronLogFormatterTest {
  public Object getTagValue(List<Trace.KeyValue> tags, String key) {
    for (Trace.KeyValue tag : tags) {
      if (tag.getKey().equals(key)) {
        Schema.SchemaFieldType schemaFieldType = tag.getFieldType();
        if (schemaFieldType.equals(Schema.SchemaFieldType.STRING)
            || schemaFieldType.equals(Schema.SchemaFieldType.KEYWORD)) {
          return tag.getVStr();
        }
        if (schemaFieldType.equals(Schema.SchemaFieldType.INTEGER)) {
          return tag.getVInt32();
        }
        if (schemaFieldType.equals(Schema.SchemaFieldType.LONG)) {
          return tag.getVInt64();
        }
        if (schemaFieldType.equals(Schema.SchemaFieldType.DOUBLE)) {
          return tag.getVFloat64();
        }
        if (schemaFieldType.equals(Schema.SchemaFieldType.BOOLEAN)) {
          return tag.getVBool();
        }
        if (schemaFieldType.equals(Schema.SchemaFieldType.BINARY)) {
          return tag.getVBinary();
        }
      }
    }
    return null;
  }

  @Test
  public void testApiSpanConversion() throws JsonProcessingException {
    String message =
        "{\"ip_address\":\"3.86.63.133\",\"http_method\":\"POST\",\"method\":true,\"enterprise\":\"E012Y1ZD5PU\",\"team\":\"T012YTS8XKM\",\"user\":\"U012YTS942X\",\"status\":\"ok\",\"http_params\":\"flannel_host=flannelbe-dev-iad-iaz2&include_permissions=falseth\",\"ua\":\"Slack-Flannel-Web\\/vef2bd:4046\",\"unique_id\":\"YB2RcPgcUv7PCuIbo8posQAAoDg\",\"request_queue_time\":2262,\"microtime_elapsed\":1418,\"mysql_query_count\":0,\"mysql_query_time\":0,\"mysql_conns_count\":0,\"mysql_conns_time\":0,\"mysql_rows_count\":0,\"mysql_rows_affected\":0,\"mc_queries_count\":11,\"mc_queries_time\":6782,\"frl_time\":0,\"init_time\":1283,\"api_dispatch_time\":0,\"api_output_time\":0,\"api_output_size\":0,\"api_strict\":false,\"ekm_decrypt_reqs_time\":0,\"ekm_decrypt_reqs_count\":0,\"ekm_encrypt_reqs_time\":0,\"ekm_encrypt_reqs_count\":0,\"grpc_req_count\":0,\"grpc_req_time\":0,\"agenda_req_count\":0,\"agenda_req_time\":0,\"trace\":\"#route_main() -> lib_controller.php:69#Controller::handlePost() -> Controller.hack:58#CallbackApiController::handleRequest() -> api.php:45#local_callbacks_api_main_inner() -> api.php:250#api_dispatch() -> lib_api.php:179#api_callbacks_flannel_verifyToken() -> api__callbacks_flannel.php:1714#api_output_fb_thrift() -> lib_api_output.php:390#_api_output_log_call()\",\"client_connection_state\":\"unset\",\"ms_requests_count\":0,\"ms_requests_time\":0,\"token_type\":\"cookie\",\"limited_access_requester_workspace\":\"\",\"limited_access_allowed_workspaces\":\"\",\"repo_auth\":true,\"cf_id\":\"6999afc7b6:haproxy-edge-dev-iad-igu2\",\"external_user\":\"W012XXXFC\",\"timestamp\":\"2021-02-05 10:41:52.340\",\"git_sha\":\"unknown\",\"hhvm_version\":\"4.39.0\",\"slath\":\"callbacks.flannel.verifyToken\",\"php_type\":\"api\",\"webapp_cluster_pbucket\":0,\"webapp_cluster_name\":\"callbacks\",\"webapp_cluster_nest\":\"normal\",\"dev_env\":\"dev-main\",\"pay_product_level\":\"enterprise\",\"level\":\"info\"}";
    String indexName = "hhvm-api_log";
    String host = "slack-www-hhvm-dev-dev-callbacks-iad-j8zj";
    long timestamp = 1612550512340953000L;

    // Make a test message
    Murron.MurronMessage.Builder testMurronMsgBuilder = Murron.MurronMessage.newBuilder();
    testMurronMsgBuilder
        .setMessage(ByteString.copyFrom(message.getBytes(StandardCharsets.UTF_8)))
        .setType(indexName)
        .setHost(host)
        .setTimestamp(timestamp);
    Murron.MurronMessage testMurronMsg = testMurronMsgBuilder.build();

    Trace.Span apiSpan = MurronLogFormatter.fromApiLog(testMurronMsg);
    assertThat(apiSpan.getDuration()).isEqualTo(1418L);
    assertThat(apiSpan.getName()).isEqualTo(indexName);
    assertThat(apiSpan.getId().toString().contains(host)).isTrue();
    assertThat(apiSpan.getTraceId().isEmpty()).isTrue();
    assertThat(apiSpan.getParentId().isEmpty()).isTrue();
    assertThat(apiSpan.getTimestamp()).isEqualTo(timestamp / 1000);
    List<Trace.KeyValue> tags = apiSpan.getTagsList();
    assertThat(getTagValue(tags, "http_method")).isEqualTo("POST");
    assertThat(getTagValue(tags, "mysql_conns_count")).isEqualTo(0);
    assertThat((boolean) getTagValue(tags, "method")).isTrue();
    assertThat(getTagValue(tags, API_LOG_DURATION_FIELD)).isNull();
    assertThat(getTagValue(tags, LogMessage.ReservedField.PARENT_ID.fieldName)).isNull();
    assertThat(getTagValue(tags, LogMessage.ReservedField.TRACE_ID.fieldName)).isNull();
    assertThat(getTagValue(tags, LogMessage.ReservedField.HOSTNAME.fieldName)).isEqualTo(host);
    assertThat(getTagValue(tags, LogMessage.ReservedField.SERVICE_NAME.fieldName))
        .isEqualTo("hhvm-api_log");
  }

  @Test
  public void testApiSpanWithTraceContextConversion() throws JsonProcessingException {
    final String message =
        "{\"ip_address\":\"3.86.63.133\",\"trace_id\":\"123\",\"parent_id\":\"4567\",\"http_method\":\"POST\",\"method\":true,\"enterprise\":\"E012Y1ZD5PU\",\"team\":\"T012YTS8XKM\",\"user\":\"U012YTS942X\",\"status\":\"ok\",\"http_params\":\"flannel_host=flannelbe-dev-iad-iaz2&include_permissions=falseth\",\"ua\":\"Slack-Flannel-Web\\/vef2bd:4046\",\"unique_id\":\"YB2RcPgcUv7PCuIbo8posQAAoDg\",\"request_queue_time\":2262,\"microtime_elapsed\":14168,\"mysql_query_count\":0,\"mysql_query_time\":0,\"mysql_conns_count\":0,\"mysql_conns_time\":0,\"mysql_rows_count\":0,\"mysql_rows_affected\":0,\"mc_queries_count\":11,\"mc_queries_time\":6782,\"frl_time\":0,\"init_time\":1283,\"api_dispatch_time\":0,\"api_output_time\":0,\"api_output_size\":0,\"api_strict\":false,\"ekm_decrypt_reqs_time\":0,\"ekm_decrypt_reqs_count\":0,\"ekm_encrypt_reqs_time\":0,\"ekm_encrypt_reqs_count\":0,\"grpc_req_count\":0,\"grpc_req_time\":0,\"agenda_req_count\":0,\"agenda_req_time\":0,\"trace\":\"#route_main() -> lib_controller.php:69#Controller::handlePost() -> Controller.hack:58#CallbackApiController::handleRequest() -> api.php:45#local_callbacks_api_main_inner() -> api.php:250#api_dispatch() -> lib_api.php:179#api_callbacks_flannel_verifyToken() -> api__callbacks_flannel.php:1714#api_output_fb_thrift() -> lib_api_output.php:390#_api_output_log_call()\",\"client_connection_state\":\"unset\",\"ms_requests_count\":0,\"ms_requests_time\":0,\"token_type\":\"cookie\",\"limited_access_requester_workspace\":\"\",\"limited_access_allowed_workspaces\":\"\",\"repo_auth\":true,\"cf_id\":\"6999afc7b6:haproxy-edge-dev-iad-igu2\",\"external_user\":\"W012XXXFC\",\"timestamp\":\"2021-02-05 10:41:52.340\",\"git_sha\":\"unknown\",\"hhvm_version\":\"4.39.0\",\"slath\":\"callbacks.flannel.verifyToken\",\"php_type\":\"api\",\"webapp_cluster_pbucket\":0,\"webapp_cluster_name\":\"callbacks\",\"webapp_cluster_nest\":\"normal\",\"dev_env\":\"dev-main\",\"pay_product_level\":\"enterprise\",\"type\":\"api_log\",\"level\":\"info\"}";
    final String indexName = "hhvm-api_log";
    final String host = "slack-www-hhvm-dev-dev-callbacks-iad-j8zj";
    final long timestamp = 1612550512340953000L;

    // Make a test message
    Murron.MurronMessage.Builder testMurronMsgBuilder = Murron.MurronMessage.newBuilder();
    testMurronMsgBuilder
        .setMessage(ByteString.copyFrom(message.getBytes(StandardCharsets.UTF_8)))
        .setType(indexName)
        .setHost(host)
        .setTimestamp(timestamp);
    Murron.MurronMessage testMurronMsg = testMurronMsgBuilder.build();

    Trace.Span apiSpan = MurronLogFormatter.fromApiLog(testMurronMsg);
    assertThat(apiSpan.getName()).isEqualTo("api_log");
    assertThat(apiSpan.getDuration()).isEqualTo(14168L);
    assertThat(apiSpan.getId().toString().contains(host)).isTrue();
    assertThat(apiSpan.getTraceId().toStringUtf8()).isEqualTo("123");
    assertThat(apiSpan.getParentId().toStringUtf8()).isEqualTo("4567");
    assertThat(apiSpan.getTimestamp()).isEqualTo(timestamp / 1000);
    List<Trace.KeyValue> tags = apiSpan.getTagsList();
    assertThat(getTagValue(tags, "http_method")).isEqualTo("POST");
    assertThat(getTagValue(tags, "mysql_conns_count")).isEqualTo(0);
    assertThat((boolean) getTagValue(tags, "method")).isTrue();
    assertThat(getTagValue(tags, API_LOG_DURATION_FIELD)).isNull();
    assertThat(getTagValue(tags, LogMessage.ReservedField.PARENT_ID.fieldName)).isNull();
    assertThat(getTagValue(tags, LogMessage.ReservedField.TRACE_ID.fieldName)).isNull();
    assertThat(getTagValue(tags, LogMessage.ReservedField.HOSTNAME.fieldName)).isEqualTo(host);
    assertThat(getTagValue(tags, LogMessage.ReservedField.SERVICE_NAME.fieldName))
        .isEqualTo("hhvm-api_log");
  }

  @Test
  public void testNullMurronMessage() throws JsonProcessingException {
    MurronLogFormatter.fromApiLog(null);
  }

  @Test
  public void testEmptyMurronMessage() {
    assertThatExceptionOfType(MismatchedInputException.class)
        .isThrownBy(() -> MurronLogFormatter.fromApiLog(Murron.MurronMessage.newBuilder().build()));
  }

  @Test
  public void testEnvoySpanWithTraceContextConversion() throws JsonProcessingException {
    // duration as a string
    testEnvoySpanWithTraceContextConversion(
        "{\"unique_id\":\"d009c63c-be2f-47fa-8c53\",\"http_header_upgrade\":null,\"grpc_status\":null,\"tcpip_remote_ip\":\"10.232.200.31\",\"duration\":\"36\",\"req_method\":\"POST\",\"tcpip_local_with_port\":\"10.219.290.121:81\",\"tcpip_remote_with_port\":\"101.22.228.30:36884\",\"upstream_hostname\":\"slack-www-hhvm-main-iad-sss2\",\"downstream_host_with_port\":\"10.47.47.46:0\",\"request_id\":\"d009c63c-be2f-47fa-8c53\",\"path\":\"/api/eventlog.history\",\"bytes_sent\":69,\"region\":\"us-east-1\",\"http_header_connection\":null,\"zone\":\"us-east-1g\",\"timestamp\":\"2022-05-27T18:15:10.971Z\",\"authority\":\"slack.com\",\"bytes_received\":325,\"listener\":\"nebula\",\"forwarder\":\"127.47.47.40\",\"app\":\"envoy-www\",\"response_code\":200,\"service_time\":\"35\",\"protocol\":\"HTTP/1.1\",\"platform\":\"chef\",\"tcpip_local_ip\":\"10.1.1.1\",\"user_agent\":\"com.t.co/22.05.10 (iPhone; iOS 15.4.1; Scale/3.00)\",\"route_name\":\"main_default\",\"upstream_cluster\":\"main_normal\",\"zone_id\":\"use1-az4\",\"response_flags\":\"via_upstream:-\",\"grpc_unique_id\":null}",
        true);
    // duration as a int
    testEnvoySpanWithTraceContextConversion(
        "{\"unique_id\":\"d009c63c-be2f-47fa-8c53\",\"http_header_upgrade\":null,\"grpc_status\":null,\"tcpip_remote_ip\":\"10.232.200.31\",\"duration\":36,\"req_method\":\"POST\",\"tcpip_local_with_port\":\"10.219.290.121:81\",\"tcpip_remote_with_port\":\"101.22.228.30:36884\",\"upstream_hostname\":\"slack-www-hhvm-main-iad-sss2\",\"downstream_host_with_port\":\"10.47.47.46:0\",\"request_id\":\"d009c63c-be2f-47fa-8c53\",\"path\":\"/api/eventlog.history\",\"bytes_sent\":69,\"region\":\"us-east-1\",\"http_header_connection\":null,\"zone\":\"us-east-1g\",\"timestamp\":\"2022-05-27T18:15:10.971Z\",\"authority\":\"slack.com\",\"bytes_received\":325,\"listener\":\"nebula\",\"forwarder\":\"127.47.47.40\",\"app\":\"envoy-www\",\"response_code\":200,\"service_time\":\"35\",\"protocol\":\"HTTP/1.1\",\"platform\":\"chef\",\"tcpip_local_ip\":\"10.1.1.1\",\"user_agent\":\"com.t.co/22.05.10 (iPhone; iOS 15.4.1; Scale/3.00)\",\"route_name\":\"main_default\",\"upstream_cluster\":\"main_normal\",\"zone_id\":\"use1-az4\",\"response_flags\":\"via_upstream:-\",\"grpc_unique_id\":null}",
        false);
  }

  private void testEnvoySpanWithTraceContextConversion(String message, boolean isDurationAsString)
      throws JsonProcessingException {
    final String indexName = "envoy";
    final String host = "myserver-server-j8zj";
    final long timestamp = 1612550512340953000L;
    final String requestId = "d009c63c-be2f-47fa-8c53";

    // Make a test message
    Murron.MurronMessage.Builder testMurronMsgBuilder = Murron.MurronMessage.newBuilder();
    testMurronMsgBuilder
        .setMessage(ByteString.copyFrom(message.getBytes(StandardCharsets.UTF_8)))
        .setType(indexName)
        .setHost(host)
        .setTimestamp(timestamp);
    Murron.MurronMessage testMurronMsg = testMurronMsgBuilder.build();

    Trace.Span apiSpan = MurronLogFormatter.fromEnvoyLog(testMurronMsg);
    assertThat(apiSpan.getName()).isEqualTo(indexName);
    assertThat(apiSpan.getDuration()).isEqualTo(36000L);
    assertThat(apiSpan.getId().toStringUtf8()).isEqualTo(requestId);
    assertThat(apiSpan.getTraceId().toStringUtf8()).isEqualTo(requestId);
    assertThat(apiSpan.getParentId().toStringUtf8()).isEmpty();
    assertThat(apiSpan.getTimestamp()).isEqualTo(timestamp / 1000);
    List<Trace.KeyValue> tags = apiSpan.getTagsList();
    assertThat(getTagValue(tags, "tcpip_remote_ip")).isEqualTo("10.232.200.31");
    assertThat(getTagValue(tags, "req_method")).isEqualTo("POST");
    assertThat(getTagValue(tags, "bytes_sent")).isEqualTo(69);
    assertThat(getTagValue(tags, "path")).isEqualTo("/api/eventlog.history");
    if (isDurationAsString) {
      assertThat(getTagValue(tags, ENVOY_DURATION_FIELD)).isEqualTo("36");
    } else {
      assertThat(getTagValue(tags, ENVOY_DURATION_FIELD)).isEqualTo(36);
    }
    assertThat(getTagValue(tags, LogMessage.ReservedField.PARENT_ID.fieldName)).isNull();
    assertThat(getTagValue(tags, LogMessage.ReservedField.TRACE_ID.fieldName)).isNull();
    assertThat(getTagValue(tags, LogMessage.ReservedField.HOSTNAME.fieldName)).isEqualTo(host);
    assertThat(getTagValue(tags, LogMessage.ReservedField.SERVICE_NAME.fieldName))
        .isEqualTo(indexName);
  }
}
