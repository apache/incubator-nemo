package edu.snu.vortex.runtime;

import java.io.Serializable;
import java.util.List;

public abstract class Task implements Serializable {
  private final List<Channel> inChans;
  private final List<Channel> outChan;

  public Task(final List<Channel> inChans,
              final List<Channel> outChan) {
    this.inChans = inChans;
    this.outChan = outChan;
  }

  public abstract void compute();

  public List<Channel> getInChans() {
    return inChans;
  }

  public List<Channel> getOutChans() {
    return outChan;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("inChans: ");
    if (inChans != null) {
      sb.append(inChans.toString());
    }
    sb.append(" / outChan: ");
    sb.append(outChan.toString());
    return sb.toString();
  }
}
