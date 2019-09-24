package org.apache.nemo.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class TaskMetrics {

  private final int window = 30;
  private long updatedTime;

  private final AtomicLong inputElement;

  private final AtomicLong outputElement;

  private final AtomicLong computation;

  public TaskMetrics() {
    this.inputElement = new AtomicLong();
    this.outputElement = new AtomicLong();
    this.computation = new AtomicLong();
    this.updatedTime = System.currentTimeMillis();
  }

  public void incrementInputElement() {
    inputElement.incrementAndGet();
  }

  public void incrementOutputElement() {
    outputElement.incrementAndGet();
  }

  public void incrementComputation(final long update) {
    computation.addAndGet(update);
  }

  public RetrievedMetrics retrieve() {
    final long ie = inputElement.get();
    final long oe = outputElement.get();
    final long c = computation.get();

    final long currTime = System.currentTimeMillis();

    if (currTime - updatedTime >= TimeUnit.SECONDS.toMillis(window)) {

      final long elapsed = (currTime - TimeUnit.SECONDS.toMillis(window)) - updatedTime;

      updatedTime = currTime - TimeUnit.SECONDS.toMillis(window);

      long updateIe = ie / window * (elapsed / 1000);
      long updateOe = oe / window * (elapsed / 1000);
      long updateComp = c / window * (elapsed / 1000);

      inputElement.getAndAdd(-updateIe);
      outputElement.getAndAdd(-updateOe);
      computation.getAndAdd(-updateComp);
    }

    return new RetrievedMetrics(ie, oe, c);
  }

  public static final class RetrievedMetrics {
    public final long inputElement;
    public final long outputElement;
    public final long computation;

    public RetrievedMetrics(final long inputElement,
                            final long outputElement,
                            final long computation) {
      this.inputElement = inputElement;
      this.outputElement = outputElement;
      this.computation = computation;
    }
  }
}
