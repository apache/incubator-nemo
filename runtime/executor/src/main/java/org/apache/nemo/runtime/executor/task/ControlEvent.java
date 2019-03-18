package org.apache.nemo.runtime.executor.task;

import java.util.Optional;

public final class ControlEvent {
  private final ControlMessageType controlMessageType;
  private final String dstVertexId;

  public enum ControlMessageType {
    FLUSH_LATENCY
  }

  public ControlEvent(final ControlMessageType msgType,
                      final String dstVertexId) {
    this.controlMessageType = msgType;
    this.dstVertexId = dstVertexId;
  }

  public String getDstVertexId() {
    return dstVertexId;
  }

  public ControlMessageType getControlMessageType() {
    return controlMessageType;
  }
}
