package org.apache.nemo.runtime.executor.common.relayserverclient;

import org.apache.nemo.runtime.executor.common.datatransfer.DataFrameEncoder;

public final class RelayDataFrame {

  public final String dstId;
  public final DataFrameEncoder.DataFrame dataFrame;

  public RelayDataFrame(final String edgeId,
                        final int taskIndex,
                        final boolean inContext,
                        final DataFrameEncoder.DataFrame dataFrame) {
    this.dstId = RelayUtils.createId(edgeId, taskIndex, inContext);
    this.dataFrame = dataFrame;
  }

  public RelayDataFrame(final String dstId,
                        final DataFrameEncoder.DataFrame dataFrame) {
    this.dstId = dstId;
    this.dataFrame = dataFrame;
  }
}
