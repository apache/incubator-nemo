package org.apache.nemo.common.nettyvm;

import java.io.Serializable;

public final class ContainsStateResponse implements Serializable {
  public final String taskId;
  public final boolean result;
  public ContainsStateResponse(final String taskId,
                               final boolean result) {
    this.taskId = taskId;
    this.result = result;
  }
}
