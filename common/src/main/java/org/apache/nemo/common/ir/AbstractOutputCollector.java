package org.apache.nemo.common.ir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractOutputCollector<O> implements OutputCollector<O> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractOutputCollector.class.getName());

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
  }

  @Override
  public void disableOffloading() {
    endOffloading = true;
  }
}
