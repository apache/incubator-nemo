package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.runtime.common.comm.ControlMessage;

import java.util.Optional;

public final class OffloadingControlEvent {
  private final ControlMessageType controlMessageType;

  private final Optional<Object> data;
  private final String dstVertexId;

  public enum ControlMessageType {
    START_OFFLOADING,
    STOP_OFFLOADING,
    FLUSH
  }

  public OffloadingControlEvent(final ControlMessageType msgType,
                                final String dstVertexId,
                                final Object data) {
    this.controlMessageType = msgType;
    this.data = Optional.of(data);
    this.dstVertexId = dstVertexId;
  }

  public OffloadingControlEvent(final ControlMessageType msgType,
                                final String dstVertexId) {
    this.controlMessageType = msgType;
    this.data = Optional.empty();
    this.dstVertexId = dstVertexId;
  }

  public String getDstVertexId() {
    return dstVertexId;
  }

  public ControlMessageType getControlMessageType() {
    return controlMessageType;
  }

  public Optional<Object> getData() {
    return data;
  }
}
