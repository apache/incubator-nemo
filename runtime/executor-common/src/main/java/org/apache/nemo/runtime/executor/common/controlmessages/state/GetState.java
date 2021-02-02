package org.apache.nemo.runtime.executor.common.controlmessages.state;

import java.io.Serializable;

public final class GetState implements Serializable {
  public final String taskId;
  public GetState(final String taskId) {
    this.taskId = taskId;
  }
}
