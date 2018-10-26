package org.apache.nemo.compiler.frontend.spark.transform;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.spark.api.java.function.Function;

/**
 * Map Transform for Spark.
 * @param <I> input type.
 * @param <O> output type.
 */
public final class MapTransform<I, O> implements Transform<I, O> {
  private final Function<I, O> func;
  private OutputCollector<O> outputCollector;

  /**
   * Constructor.
   * @param func the function to run map with.
   */
  public MapTransform(final Function<I, O> func) {
    this.func = func;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<O> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final I element) {
      try {
        outputCollector.emit(func.call(element));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
  }

  @Override
  public void close() {
  }
}
