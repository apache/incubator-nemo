package edu.snu.vortex.runtime;

import java.io.Serializable;

public class Task implements Serializable {
  private final Channel inChan;
  private final UserFunction userFunction;
  private final Channel outChan;

  public Task(final Channel inChan,
              final UserFunction userFunction,
              final Channel outChan) {
    this.inChan = inChan;
    this.userFunction = userFunction;
    this.outChan = outChan;
  }

  public void compute() {
    if (inChan == null) {
      outChan.write(userFunction.func(null));
    } else {
      outChan.write(userFunction.func(inChan.read()));
    }
  }

  public Channel getInChan() {
    return inChan;
  }

  public Channel getOutChan() {
    return outChan;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("inChan: ");
    if (inChan != null) {
      sb.append(inChan.toString());
    }
    sb.append(" / userFun: ");
    sb.append(userFunction.toString());
    sb.append(" / outChan: ");
    sb.append(outChan.toString());
    return sb.toString();
  }
}
