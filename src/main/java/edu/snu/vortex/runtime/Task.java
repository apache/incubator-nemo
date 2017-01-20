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
    outChan.write(userFunction.func(inChan.read()));
  }

  public Channel getInChan() {
    return inChan;
  }

  public Channel getOutChan() {
    return outChan;
  }
}
