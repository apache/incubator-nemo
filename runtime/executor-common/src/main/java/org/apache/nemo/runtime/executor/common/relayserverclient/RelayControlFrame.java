package org.apache.nemo.runtime.executor.common.relayserverclient;

import org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContextSetupMessage;

public final class RelayControlFrame {

  public final String dstId;
  public final ByteTransferContextSetupMessage controlMsg;

  public RelayControlFrame(final String edgeId,
                           final int taskIndex,
                           final boolean inContext,
                           final ByteTransferContextSetupMessage controlMsg) {
    this.dstId = RelayUtils.createId(edgeId, taskIndex, inContext);
    this.controlMsg = controlMsg;
  }

  public RelayControlFrame(final String dstId,
                           final ByteTransferContextSetupMessage controlMsg) {
    this.dstId = dstId;
    this.controlMsg = controlMsg;
  }
}
