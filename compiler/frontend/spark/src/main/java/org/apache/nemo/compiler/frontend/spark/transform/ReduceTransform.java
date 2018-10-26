package org.apache.nemo.compiler.frontend.spark.transform;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.spark.api.java.function.Function2;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Reduce Transform for Spark.
 *
 * @param <T> element type.
 */
public final class ReduceTransform<T> implements Transform<T, T> {
  private final Function2<T, T, T> func;
  private OutputCollector<T> outputCollector;
  private T result;

  /**
   * Constructor.
   * @param func function to run for the reduce transform.
   */
  public ReduceTransform(final Function2<T, T, T> func) {
    this.func = func;
    this.result = null;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final T element) {
    if (element == null) { // nothing to be done.
      return;
    }

    try {
      if (result == null) {
        result = element;
      }

      result = func.call(result, element);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    outputCollector.emit(result);
  }

  /**
   * Reduce the iterator elements into a single object.
   * @param elements the iterator of elements.
   * @param func function to apply for reduction.
   * @param <T> type of the elements.
   * @return the reduced element.
   */
  @Nullable
  public static <T> T reduceIterator(final Iterator<T> elements, final Function2<T, T, T> func) {
    if (!elements.hasNext()) { // nothing to be done
      return null;
    }

    T res = elements.next();
    while (elements.hasNext()) {
      try {
        res = func.call(res, elements.next());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return res;
  }

  @Override
  public void close() {
  }
}
