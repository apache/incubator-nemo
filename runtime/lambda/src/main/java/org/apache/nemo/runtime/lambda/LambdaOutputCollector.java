package org.apache.nemo.runtime.lambda;

import java.io.Serializable;

public interface LambdaOutputCollector<O> extends Serializable {
  /**
   * Single-destination emit.
   * @param output value.
   */
  void emit(O output);
}

