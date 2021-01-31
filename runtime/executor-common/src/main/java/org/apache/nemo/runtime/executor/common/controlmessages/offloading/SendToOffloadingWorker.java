package org.apache.nemo.runtime.executor.common.controlmessages.offloading;

public final class SendToOffloadingWorker {

  public final String taskId;

  public SendToOffloadingWorker(final String taskId) {
    this.taskId = taskId;
  }
}
