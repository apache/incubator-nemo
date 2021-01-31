package org.apache.nemo.runtime.executor.task.util;

import java.io.Serializable;

public final class TCPResponseHasEvent implements Serializable {

  public final boolean hasEvent;
  public TCPResponseHasEvent(final boolean hasEvent) {
    this.hasEvent = hasEvent;
  }
}
