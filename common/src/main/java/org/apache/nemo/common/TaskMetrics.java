package org.apache.nemo.common;

import java.util.concurrent.atomic.AtomicLong;

public final class TaskMetrics {

  public final AtomicLong inputElement;
  public final AtomicLong outputElement;

  public TaskMetrics() {
    this.inputElement = new AtomicLong();
    this.outputElement = new AtomicLong();
  }
}
