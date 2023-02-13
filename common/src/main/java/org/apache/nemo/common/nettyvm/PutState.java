package org.apache.nemo.common.nettyvm;

import java.io.Serializable;

public final class PutState implements Serializable {
  public final String taskId;
  public final byte[] bytes;
  public PutState(final String taskId,
                  final byte[] bytes) {
    this.taskId = taskId;
    this.bytes = bytes;
  }
}