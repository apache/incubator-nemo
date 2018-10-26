package org.apache.nemo.common.ir.vertex.transform;

import org.apache.nemo.common.ir.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;

/**
 * A {@link Transform} that collects task-level statistics used for dynamic optimization.
 * The collected statistics is sent to vertex with {@link AggregateMetricTransform} as a tagged output
 * when this transform is closed.
 *
 * @param <I> input type.
 * @param <O> output type.
 */
public final class MetricCollectTransform<I, O> implements Transform<I, O> {
  private static final Logger LOG = LoggerFactory.getLogger(MetricCollectTransform.class.getName());
  private OutputCollector<O> outputCollector;
  private O dynOptData;
  private final BiFunction<Object, O, O> dynOptDataCollector;
  private final BiFunction<O, OutputCollector, O> closer;

  /**
   * MetricCollectTransform constructor.
   */
  public MetricCollectTransform(final O dynOptData,
                                final BiFunction<Object, O, O> dynOptDataCollector,
                                final BiFunction<O, OutputCollector, O> closer) {
    this.dynOptData = dynOptData;
    this.dynOptDataCollector = dynOptDataCollector;
    this.closer = closer;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<O> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final I element) {
    dynOptData = dynOptDataCollector.apply(element, dynOptData);
  }

  @Override
  public void close() {
    closer.apply(dynOptData, outputCollector);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(MetricCollectTransform.class);
    sb.append(":");
    sb.append(super.toString());
    return sb.toString();
  }
}
