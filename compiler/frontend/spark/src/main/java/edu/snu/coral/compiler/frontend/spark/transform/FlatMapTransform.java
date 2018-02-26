package edu.snu.coral.compiler.frontend.spark.transform;

import edu.snu.coral.common.ir.Pipe;
import edu.snu.coral.common.ir.vertex.transform.Transform;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flatmap Transform that flattens each output element after mapping each elements to an iterator.
 * @param <T> input type.
 * @param <U> output type.
 */
public final class FlatMapTransform<T, U> implements Transform<T, U> {
  private static final Logger LOG = LoggerFactory.getLogger(FlatMapTransform.class.getName());

  private final FlatMapFunction<T, U> func;
  private Pipe<U> pipe;

  /**
   * Constructor.
   * @param func flat map function.
   */
  public FlatMapTransform(final FlatMapFunction<T, U> func) {
    this.func = func;
  }

  @Override
  public void prepare(final Context context, final Pipe<U> p) {
    this.pipe = p;
  }

  @Override
  public void onData(final Object element) {
    try {
      func.call((T) element).forEachRemaining(data -> {
        LOG.info("FlatMapTransform onData emitting {}", data);
        pipe.emit(data);
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
  }
}
