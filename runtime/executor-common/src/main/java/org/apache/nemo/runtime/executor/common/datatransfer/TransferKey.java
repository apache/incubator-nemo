package org.apache.nemo.runtime.executor.common.datatransfer;

import java.io.Serializable;
import java.util.Objects;

public final class TransferKey implements Serializable {
    public final String edgeId;
    public final int srcTaskIndex;
    public final int dstTaskIndex;
    public final boolean isOutputTransfer;

    public TransferKey(final String edgeId,
                       final int srcTaskIndex,
                       final int dstTaskIndex,
                       final boolean isOutputTransfer) {
      this.edgeId = edgeId;
      this.srcTaskIndex = srcTaskIndex;
      this.dstTaskIndex = dstTaskIndex;
      this.isOutputTransfer = isOutputTransfer;
    }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TransferKey that = (TransferKey) o;
    return srcTaskIndex == that.srcTaskIndex &&
      dstTaskIndex == that.dstTaskIndex &&
      isOutputTransfer == that.isOutputTransfer &&
      Objects.equals(edgeId, that.edgeId);
  }

  @Override
  public int hashCode() {

    return Objects.hash(edgeId, srcTaskIndex, dstTaskIndex, isOutputTransfer);
  }
}
