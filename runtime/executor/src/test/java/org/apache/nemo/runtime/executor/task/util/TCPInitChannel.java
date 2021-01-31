package org.apache.nemo.runtime.executor.task.util;

import java.io.Serializable;

public final class TCPInitChannel implements Serializable {

  public final int index;
  public TCPInitChannel(final int index) {
    this.index = index;
  }
}
