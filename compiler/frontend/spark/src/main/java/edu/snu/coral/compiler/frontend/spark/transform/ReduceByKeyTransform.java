package edu.snu.coral.compiler.frontend.spark.transform;

import edu.snu.coral.common.ir.Pipe;
import edu.snu.coral.common.ir.vertex.transform.Transform;
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
  private Pipe<Tuple2<K, V>> pipe;

  /**
   * Constructor.
   * @param func reduce function.
   */
  public ReduceByKeyTransform(final Function2<V, V, V> func) {
    this.func = func;
    this.keyToValues = new HashMap<>();
  }

  @Override
  public void prepare(final Context context, final Pipe<Tuple2<K, V>> p) {
    this.pipe = p;
  }

  @Override
  public void onData(final Object element) {
    K key = ((Tuple2<K, V>) element)._1;
    V value = ((Tuple2<K, V>) element)._2;

    keyToValues.putIfAbsent(key, new ArrayList<>());
    keyToValues.get(key).add(value);
    LOG.info("ReduceByKeyTransform onData adding {} {}", key, value);
  }

  @Override
  public void close() {
    keyToValues.entrySet().stream().map(entry -> {
      final V value = ReduceTransform.reduceIterator(entry.getValue().iterator(), func);
      return new Tuple2<>(entry.getKey(), value);
    }).forEach(data -> {
      pipe.emit(data);
      LOG.info("ReduceByKeyTransform close emitting {}", data);
    });
    keyToValues.clear();
  }
}
