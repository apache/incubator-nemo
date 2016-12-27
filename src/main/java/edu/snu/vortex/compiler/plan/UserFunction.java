package edu.snu.vortex.compiler.plan;

public interface UserFunction<I, O> {
  O apply(I input);
}
