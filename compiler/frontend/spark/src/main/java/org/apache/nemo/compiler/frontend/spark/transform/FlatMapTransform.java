package org.apache.nemo.compiler.frontend.spark.transform;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * Flatmap Transform that flattens each output element after mapping each elements to an iterator.
 * @param <T> input type.
 * @param <U> output type.
 */
public final class FlatMapTransform<T, U> implements Transform<T, U> {
  private final FlatMapFunction<T, U> func;
  private OutputCollector<U> outputCollector;

  /**
   * Constructor.
   * @param func flat map function.
   */
  public FlatMapTransform(final FlatMapFunction<T, U> func) {
    this.func = func;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<U> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final T element) {
    try {
      func.call(element).forEachRemaining(outputCollector::emit);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
  }
}
