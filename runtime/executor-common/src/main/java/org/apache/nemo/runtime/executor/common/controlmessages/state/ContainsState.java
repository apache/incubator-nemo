package org.apache.nemo.runtime.executor.common.controlmessages.state;

import java.io.Serializable;

public final class ContainsState implements Serializable {
  public final String taskId;
  public ContainsState(final String taskId) {
    this.taskId = taskId;
  }
}
