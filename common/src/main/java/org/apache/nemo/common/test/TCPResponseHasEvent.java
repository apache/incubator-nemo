package org.apache.nemo.common.test;

import java.io.Serializable;

public final class TCPResponseHasEvent implements Serializable {

  public final boolean hasEvent;
  public TCPResponseHasEvent(final boolean hasEvent) {
    this.hasEvent = hasEvent;
  }
}
