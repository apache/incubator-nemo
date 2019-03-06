package org.apache.nemo.common.ir;

public abstract class AbstractOutputCollector<O> implements OutputCollector<O> {

  protected long inputTimestamp;
  protected volatile boolean startOffloading;
  protected volatile boolean endOffloading;
  protected volatile boolean offloading;

  @Override
  public void setInputTimestamp(final long timestamp) {
    inputTimestamp = timestamp;
  }

  @Override
  public long getInputTimestamp() {
    return inputTimestamp;
  }

  @Override
  public void enableOffloading() {
    startOffloading = true;
    offloading = true;
  }

  @Override
  public void disableOffloading() {
    offloading = false;
    endOffloading = true;
  }
}
