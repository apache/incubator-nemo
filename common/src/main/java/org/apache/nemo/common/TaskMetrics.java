package org.apache.nemo.common;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public final class TaskMetrics {

  private final int window = 20;
  private long updatedTime;

  private long inputElement;
  private long outputElement;
  private long computation;
  private long inbytes;
  private long outbytes;
  private long deserializedTime;
  private long serializedTime;

  public TaskMetrics() {
    this.inputElement = 0;
    this.outputElement = 0;
    this.computation = 0;
    this.inbytes = 0;
    this.outbytes = 0;
    this.deserializedTime = 0;
    this.serializedTime = 0;
    this.updatedTime = 0;
  }

  public void incrementInputElement() {
    inputElement += 1;
  }

  public void incrementOutputElement() {
    outputElement += 1;
  }

  public void incrementComputation(final long update) {
    computation += (update / 1000);
  }

  public void incrementInBytes(final long update) {
    inbytes += update;
  }

  public void incrementOutBytes(final long update) {
    outbytes +=  update;
  }

  public void incrementDeserializedTime(final long updated) {
    deserializedTime += (updated / 1000);
  }

  public void incrementSerializedTime(final long updated) {
    serializedTime += (updated / 1000);
  }

  private RetrievedMetrics prevMetric = new RetrievedMetrics(0, 0, 0, 0, 0, 0, 0);

  public RetrievedMetrics retrieve() {
    final long ie = inputElement;
    final long oe = outputElement;
    final long c = computation;
    final long ib = inbytes;
    final long dst = deserializedTime;
    final long ob = outbytes;
    final long st = serializedTime;

    final RetrievedMetrics newMetric = new RetrievedMetrics(ie, oe, c, ib, dst, ob, st);

    final RetrievedMetrics delta = delta(newMetric, prevMetric);
    prevMetric = newMetric;
    return delta;

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
  }

  public static RetrievedMetrics delta(RetrievedMetrics newMetric, RetrievedMetrics oldMetric) {
    return new RetrievedMetrics(newMetric.inputElement - oldMetric.inputElement,
      newMetric.outputElement - oldMetric.outputElement,
      newMetric.computation - oldMetric.computation,
      newMetric.inbytes - oldMetric.inbytes,
      newMetric.serializedTime - oldMetric.serializedTime,
      newMetric.outbytes - oldMetric.outbytes,
      newMetric.deserTime - oldMetric.deserTime);
  }

  private long avgCnt(final List<Long> l) {
    return l.stream().reduce(0L, (x,y) -> x+y) / l.size();
  }

  public static final class RetrievedMetrics {
    public final long inputElement;
    public final long outputElement;
    public final long computation;
    public final long inbytes;
    public final long serializedTime;
    public final long outbytes;
    public final long deserTime;

    public RetrievedMetrics(final long inputElement,
                            final long outputElement,
                            final long computation,
                            final long inbytes,
                            final long serializedTime,
                            final long outbytes,
                            final long deserTime) {
      this.inputElement = inputElement;
      this.outputElement = outputElement;
      this.computation = computation;
      this.inbytes = inbytes;
      this.serializedTime = serializedTime;
      this.outbytes = outbytes;
      this.deserTime = deserTime;
    }
  }
}
