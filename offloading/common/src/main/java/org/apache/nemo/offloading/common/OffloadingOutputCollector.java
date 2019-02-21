package org.apache.nemo.offloading.common;

import java.io.Serializable;

public interface OffloadingOutputCollector<O> extends Serializable {
  /**
   * Single-destination emit.
   * @param output value.
   */
  void emit(O output);
}
