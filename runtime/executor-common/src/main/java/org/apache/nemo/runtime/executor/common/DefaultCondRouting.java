package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.ir.vertex.transform.Transform;

import javax.inject.Inject;

public final class DefaultCondRouting implements Transform.ConditionalRouting {

  private volatile boolean toPartial = false;
  private volatile double percent = 0.0;

  @Inject
  private DefaultCondRouting() {
    // TODO: current state를 master로부터 가져와야함.
  }

  @Override
  public boolean toPartial() {
    return toPartial;
  }

  @Override
  public double getPercent() {
    return percent;
  }

  public void setPartial(final boolean p) {
    this.toPartial = p;
  }

  public void setPercent(final double p) {
    this.percent = p;
  }
}
