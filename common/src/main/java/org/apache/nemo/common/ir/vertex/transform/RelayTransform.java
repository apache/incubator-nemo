package org.apache.nemo.common.ir.vertex.transform;

import org.apache.nemo.common.ir.OutputCollector;

/**
 * A {@link Transform} relays input data from upstream vertex to downstream vertex promptly.
 * This transform can be used for merging input data into the {@link OutputCollector}.
 * @param <T> input/output type.
 */
public final class RelayTransform<T> implements Transform<T, T> {
  private OutputCollector<T> outputCollector;

  /**
   * Default constructor.
   */
  public RelayTransform() {
    // Do nothing.
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
    // Do nothing.
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(RelayTransform.class);
    sb.append(":");
    sb.append(super.toString());
    return sb.toString();
  }
}
