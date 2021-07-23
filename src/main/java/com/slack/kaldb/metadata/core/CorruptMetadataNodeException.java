package com.slack.kaldb.metadata.core;

public class CorruptMetadataNodeException extends RuntimeException {
  public CorruptMetadataNodeException(String msg) {
    super(msg);
  }

  public CorruptMetadataNodeException(String msg, Throwable t) {
    super(msg, t);
  }
}
