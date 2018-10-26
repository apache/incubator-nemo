package org.apache.nemo.common.ir;

import java.io.Serializable;

/**
 * Interface through which Transform emits outputs.
 * This is to be implemented in the runtime with
 * runtime-specific distributed data movement and storage mechanisms.
 * @param <O> output type.
 */
public interface OutputCollector<O> extends Serializable {
  /**
   * Single-destination emit.
   * @param output value.
   */
  void emit(O output);

  /**
   * Multi-destination emit.
   * Currently unused, but might come in handy
   * for operations like multi-output map.
   * @param dstVertexId destination vertex id.
   * @param output value.
   */
  <T> void emit(String dstVertexId, T output);
}
