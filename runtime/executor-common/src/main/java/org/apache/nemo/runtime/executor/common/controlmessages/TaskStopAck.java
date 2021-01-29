package org.apache.nemo.runtime.executor.common.controlmessages;

public class TaskStopAck {

  public final String srcTaskId;
  public final String edgeId;
  public final String dstTaskId;

  private TaskStopAck(final String srcTaskId,
                      final String edgeId,
                      final String dstTaskId) {
    this.srcTaskId = srcTaskId;
    this.edgeId = edgeId;
    this.dstTaskId = dstTaskId;
  }
}
