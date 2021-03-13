package org.apache.nemo.common.nettyvm;

import java.io.Serializable;

public final class PutStateResponse implements Serializable {
  public final String taskId;
  public PutStateResponse(final String taskId) {
    this.taskId = taskId;
  }
}
