package edu.snu.nemo.compiler.frontend.spark.transform;

import edu.snu.nemo.common.ir.OutputCollector;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;

/**
 * Flatmap Transform that flattens each output element after mapping each elements to an iterator.
 * @param <T> input type.
 * @param <U> output type.
 */
public final class FlatMapTransform<T, U> implements Transform<T, U> {
  private final FlatMapFunction<T, U> func;
  private OutputCollector<U> oc;

  /**
   * Constructor.
   * @param func flat map function.
   */
  public FlatMapTransform(final FlatMapFunction<T, U> func) {
    this.func = func;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<U> outputCollector) {
    this.oc = outputCollector;
  }

  @Override
  public void onData(final Iterator<T> elements, final String srcVertexId) {
    elements.forEachRemaining(element -> {
      try {
        func.call(element).forEachRemaining(oc::emit);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public void close() {
  }
}
