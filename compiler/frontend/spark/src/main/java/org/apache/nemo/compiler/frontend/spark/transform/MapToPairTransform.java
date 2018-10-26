package org.apache.nemo.compiler.frontend.spark.transform;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Map elements to Pair elements.
 * @param <T> input type.
 * @param <K> output key type.
 * @param <V> output value type.
 */
public final class MapToPairTransform<T, K, V> implements Transform<T, Tuple2<K, V>> {
  private final PairFunction<T, K, V> func;
  private OutputCollector<Tuple2<K, V>> outputCollector;

  /**
   * Constructor.
   * @param func Pair function to apply to each element.
   */
  public MapToPairTransform(final PairFunction<T, K, V> func) {
    this.func = func;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<Tuple2<K, V>> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final T element) {
    try {
      Tuple2<K, V> data = func.call(element);
      outputCollector.emit(data);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
  }
}
