package org.apache.nemo.common.ir;

public abstract class AbstractOutputCollector<O> implements OutputCollector<O> {

  protected long inputTimestamp;

  @Override
  public void setTimestamp(final long timestamp) {
    inputTimestamp = timestamp;
  }

  @Override
  public long getTimestamp() {
    return inputTimestamp;
  }
}
