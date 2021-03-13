package org.apache.nemo.common.nettyvm;

import java.io.Serializable;

public final class GetState implements Serializable {
  public final String taskId;
  public GetState(final String taskId) {
    this.taskId = taskId;
  }
}
