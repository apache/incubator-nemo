package org.apache.nemo.common;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class TaskMetrics {

  private final int window = 30;
  private long updatedTime;

  private final AtomicLong inputElement;

  private final AtomicLong outputElement;

  private final AtomicLong computation;

  private final List<Long> inputElements;
  private final List<Long> outputElements;
  private final List<Long> computations;

  public TaskMetrics() {
    this.inputElement = new AtomicLong();
    this.outputElement = new AtomicLong();
    this.computation = new AtomicLong();
    this.updatedTime = System.currentTimeMillis();

    this.inputElements = new LinkedList<>();
    this.outputElements = new LinkedList<>();
    this.computations = new LinkedList<>();
  }

  public void incrementInputElement() {
    inputElement.incrementAndGet();
  }

  public void incrementOutputElement() {
    outputElement.incrementAndGet();
  }

  public void incrementComputation(final long update) {
    computation.addAndGet(update / 1000);
  }

  public RetrievedMetrics retrieve(final int numKeys) {
    final long ie = inputElement.get();
    final long oe = outputElement.get();
    final long c = computation.get();

    inputElements.add(ie);
    outputElements.add(oe);
    computations.add(c);

    if (inputElements.size() > window) {
      inputElements.remove(0);
    }

    if (outputElements.size() > window) {
      outputElements.remove(0);
    }

    if (computations.size() > window) {
      computations.remove(0);
    }

    inputElement.getAndAdd(-ie);
    outputElement.getAndAdd(-oe);
    computation.getAndAdd(-c);

    /*
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
    */


    return new RetrievedMetrics(avgCnt(inputElements),
      avgCnt(outputElements), avgCnt(computations), numKeys);
  }

  private long avgCnt(final List<Long> l) {
    return l.stream().reduce(0L, (x,y) -> x+y) / l.size();
  }

  public static final class RetrievedMetrics {
    public final long inputElement;
    public final long outputElement;
    public final long computation;
    public final int numKeys;

    public RetrievedMetrics(final long inputElement,
                            final long outputElement,
                            final long computation,
                            final int numKeys) {
      this.inputElement = inputElement;
      this.outputElement = outputElement;
      this.computation = computation;
      this.numKeys = numKeys;
    }
  }
}
