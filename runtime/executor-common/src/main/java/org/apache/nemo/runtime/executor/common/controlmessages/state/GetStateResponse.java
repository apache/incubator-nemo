package org.apache.nemo.runtime.executor.common.controlmessages.state;

import java.io.Serializable;

public final class GetStateResponse implements Serializable {
  public final String taskId;
  public final byte[] bytes;
  public GetStateResponse(final String taskId,
                          final byte[] bytes) {
    this.taskId = taskId;
    this.bytes = bytes;
  }
}
