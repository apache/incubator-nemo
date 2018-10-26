package org.apache.nemo.runtime.common.metric;

import java.io.Serializable;

/**
 * Class for all generic event that contains timestamp at the moment.
 */
public class Event implements Serializable {
  private long timestamp;

  /**
   * Constructor.
   * @param timestamp timestamp in millisecond.
   */
  public Event(final long timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * Get timestamp.
   * @return timestamp.
   */
  public final long getTimestamp() {
    return timestamp;
  };

  /**
   * Set timestamp.
   * @param timestamp timestamp in millisecond.
   */
  public final void setTimestamp(final long timestamp) {
    this.timestamp = timestamp;
  }
}
