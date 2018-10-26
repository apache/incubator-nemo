package org.apache.nemo.common.ir.vertex.transform;

import org.apache.nemo.common.ir.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;

/**
 * A {@link Transform} that aggregates stage-level statistics sent to the master side optimizer
 * for dynamic optimization.
 *
 * @param <I> input type.
 * @param <O> output type.
 */
public final class AggregateMetricTransform<I, O> implements Transform<I, O> {
  private static final Logger LOG = LoggerFactory.getLogger(AggregateMetricTransform.class.getName());
  private OutputCollector<O> outputCollector;
  private O aggregatedDynOptData;
  private final BiFunction<Object, O, O> dynOptDataAggregator;

  /**
   * Default constructor.
   */
  public AggregateMetricTransform(final O aggregatedDynOptData,
                                  final BiFunction<Object, O, O> dynOptDataAggregator) {
    this.aggregatedDynOptData = aggregatedDynOptData;
    this.dynOptDataAggregator = dynOptDataAggregator;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<O> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final I element) {
    aggregatedDynOptData = dynOptDataAggregator.apply(element, aggregatedDynOptData);
  }

  @Override
  public void close() {
    outputCollector.emit(aggregatedDynOptData);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(AggregateMetricTransform.class);
    sb.append(":");
    sb.append(super.toString());
    return sb.toString();
  }
}
