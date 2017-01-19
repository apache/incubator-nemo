package edu.snu.vortex.runtime;

public abstract class Task<I, O> {
  public abstract O compute(I input);
}
