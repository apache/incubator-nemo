package org.apache.nemo.runtime.executor.common.relayserverclient;

import org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContextSetupMessage;

public final class RelayControlMessage {

  public enum Type {
    REGISTER,
    DEREGISTER,
  }

  public final String edgeId;
  public final int taskIndex;
  public final boolean inContext;
  public final Type type;

  public RelayControlMessage(final String edgeId,
                             final int taskIndex,
                             final boolean inContext,
                             final Type type) {
    this.edgeId = edgeId;
    this.taskIndex = taskIndex;
    this.inContext = inContext;
    this.type = type;
  }
}
