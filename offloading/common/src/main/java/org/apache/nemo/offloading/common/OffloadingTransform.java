package org.apache.nemo.offloading.common;


import java.io.Serializable;

public interface OffloadingTransform<I, O> extends Serializable {
  /**
   * Prepare the transform.
   * @param context of the transform.
   * @param outputCollector that collects outputs.
   */
  void prepare(OffloadingContext context, OffloadingOutputCollector<O> outputCollector);

  /**
   * On data received.
   * @param element data received.
   */
  void onData(I element);

  /**
   * Close the transform.
   */
  void close();

  /**
   * Context of the transform.
   */
  interface OffloadingContext extends Serializable {

  }
}
