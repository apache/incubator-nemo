package org.apache.nemo.common.nettyvm;

import java.io.Serializable;

public final class ContainsState implements Serializable {
  public final String taskId;
  public ContainsState(final String taskId) {
    this.taskId = taskId;
  }
}
