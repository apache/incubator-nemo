package org.apache.nemo.common.eventhandler;

public final class OffloadingDataEvent {
  public final String srcId;
  public final Object data;
  public final boolean isWatermark;

  public OffloadingDataEvent(final String srcId,
                             final Object data,
                             final boolean isWatermark) {
    this.srcId = srcId;
    this.data = data;
    this.isWatermark = isWatermark;
  }
}
