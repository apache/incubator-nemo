package org.apache.nemo.runtime.executor.task.util;

import java.io.Serializable;

public final class TCPRequstEvent implements Serializable {

  public final int index;
  public TCPRequstEvent(final int index) {
    this.index = index;
  }
}
