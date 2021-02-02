package org.apache.nemo.runtime.executor.common.controlmessages.state;

import java.io.Serializable;

public final class PutStateResponse implements Serializable {
  public final String taskId;
  public PutStateResponse(final String taskId) {
    this.taskId = taskId;
  }
}
