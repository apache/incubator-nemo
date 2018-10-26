package org.apache.nemo.compiler.frontend.spark.transform;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import scala.Tuple2;

import java.util.*;

/**
 * Transform for group by key transformation.
 * @param <K> key type.
 * @param <V> value type.
 */
public final class GroupByKeyTransform<K, V> implements Transform<Tuple2<K, V>, Tuple2<K, Iterable<V>>> {
  private final Map<K, List<V>> keyToValues;
  private OutputCollector<Tuple2<K, Iterable<V>>> outputCollector;

  /**
   * Constructor.
   */
  public GroupByKeyTransform() {
    this.keyToValues = new HashMap<>();
  }

  @Override
  public void prepare(final Transform.Context context, final OutputCollector<Tuple2<K, Iterable<V>>> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final Tuple2<K, V> element) {
    K key = element._1;
    V value = element._2;

    keyToValues.putIfAbsent(key, new ArrayList<>());
    keyToValues.get(key).add(value);
  }

  @Override
  public void close() {
    keyToValues.entrySet().stream().map(entry -> new Tuple2<>(entry.getKey(), (Iterable<V>) entry.getValue()))
        .forEach(outputCollector::emit);
    keyToValues.clear();
  }
}
