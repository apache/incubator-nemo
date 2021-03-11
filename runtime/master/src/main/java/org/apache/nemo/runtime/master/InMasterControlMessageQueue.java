package org.apache.nemo.runtime.master;

import org.apache.nemo.runtime.common.comm.ControlMessage;

import javax.inject.Inject;
import java.util.concurrent.LinkedBlockingQueue;

public final class InMasterControlMessageQueue {

  public final LinkedBlockingQueue<ControlMessage.Message> requestExecutorQueue;
  public final LinkedBlockingQueue<ControlMessage.Message> responseExecutorQueue;

  @Inject
  private InMasterControlMessageQueue() {
    this.requestExecutorQueue = new LinkedBlockingQueue<>();
    this.responseExecutorQueue = new LinkedBlockingQueue<>();
  }
}
