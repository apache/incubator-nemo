package org.apache.nemo.runtime.executor.task.util;

import java.io.Serializable;

public final class TCPHasEvent implements Serializable {

  public final int index;
  public TCPHasEvent(final int index) {
    this.index = index;
  }
}
