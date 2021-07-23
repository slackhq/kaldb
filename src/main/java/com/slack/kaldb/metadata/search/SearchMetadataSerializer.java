package com.slack.kaldb.metadata.search;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.kaldb.metadata.core.MetadataSerializer;
import com.slack.kaldb.proto.metadata.Metadata;

public class SearchMetadataSerializer implements MetadataSerializer<SearchMetadata> {
  private static SearchMetadata fromSearchMetadataProto(
      Metadata.SearchMetadata searchMetadataProto) {
    return new SearchMetadata(
        searchMetadataProto.getName(),
        searchMetadataProto.getSnapshotName(),
        searchMetadataProto.getUrl());
  }

  private static Metadata.SearchMetadata toSearchMetadataProto(SearchMetadata metadata) {
    return Metadata.SearchMetadata.newBuilder()
        .setName(metadata.name)
        .setSnapshotName(metadata.snapshotName)
        .setUrl(metadata.url)
        .build();
  }

  @Override
  public String toJsonStr(SearchMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toSearchMetadataProto(metadata));
  }

  @Override
  public SearchMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.SearchMetadata.Builder searchMetadataBuilder = Metadata.SearchMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, searchMetadataBuilder);
    return fromSearchMetadataProto(searchMetadataBuilder.build());
  }
}
