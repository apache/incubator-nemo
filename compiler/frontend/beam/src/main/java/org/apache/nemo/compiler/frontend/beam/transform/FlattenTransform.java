package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;

/**
 * Flatten transform implementation.
 * @param <T> input/output type.
 */
public final class FlattenTransform<T> implements Transform<T, T> {
  private OutputCollector<T> outputCollector;

  /**
   * FlattenTransform Constructor.
   */
  public FlattenTransform() {
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final T element) {
    outputCollector.emit(element);
  }

  @Override
  public void close() {
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("FlattenTransform:");
    sb.append(super.toString());
    return sb.toString();
  }
}
