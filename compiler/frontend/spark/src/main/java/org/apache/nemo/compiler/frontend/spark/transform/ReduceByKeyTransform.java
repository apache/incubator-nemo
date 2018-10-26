package org.apache.nemo.compiler.frontend.spark.transform;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

/**
 * Transform for reduce by key transformation.
 * @param <K> key type.
 * @param <V> value type.
 */
public final class ReduceByKeyTransform<K, V> implements Transform<Tuple2<K, V>, Tuple2<K, V>> {
  private static final Logger LOG = LoggerFactory.getLogger(ReduceByKeyTransform.class.getName());

  private final Map<K, List<V>> keyToValues;
  private final Function2<V, V, V> func;
  private OutputCollector<Tuple2<K, V>> outputCollector;

  /**
   * Constructor.
   * @param func reduce function.
   */
  public ReduceByKeyTransform(final Function2<V, V, V> func) {
    this.func = func;
    this.keyToValues = new HashMap<>();
  }

  @Override
  public void prepare(final Context context, final OutputCollector<Tuple2<K, V>> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final Tuple2<K, V> element) {
    final K key = element._1;
    final V value = element._2;

    keyToValues.putIfAbsent(key, new ArrayList<>());
    keyToValues.get(key).add(value);
  }

  @Override
  public void close() {
    if (keyToValues.isEmpty()) {
      LOG.warn("Spark ReduceByKeyTransform received no data!");
    } else {
      keyToValues.entrySet().stream().map(entry -> {
        final V value = ReduceTransform.reduceIterator(entry.getValue().iterator(), func);
        return new Tuple2<>(entry.getKey(), value);
      }).forEach(outputCollector::emit);
      keyToValues.clear();
    }
  }
}
