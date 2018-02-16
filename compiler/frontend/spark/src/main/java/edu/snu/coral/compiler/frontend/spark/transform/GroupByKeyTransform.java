package edu.snu.coral.compiler.frontend.spark.transform;

import edu.snu.coral.common.ir.Pipe;
import edu.snu.coral.common.ir.vertex.transform.Transform;
import scala.Tuple2;

import java.util.*;

/**
 * Transform for group by key transformation.
 * @param <K> key type.
 * @param <V> value type.
 */
public final class GroupByKeyTransform<K, V> implements Transform<Tuple2<K, V>, Tuple2<K, Iterable<V>>> {
  private final Map<K, List<V>> keyToValues;
  private Pipe<Tuple2<K, Iterable<V>>> pipe;

  /**
   * Constructor.
   */
  public GroupByKeyTransform() {
    this.keyToValues = new HashMap<>();
  }

  @Override
  public void prepare(final Transform.Context context, final Pipe<Tuple2<K, Iterable<V>>> p) {
    this.pipe = p;
  }

  @Override
  public void onData(final Object element) {
    K key = ((Tuple2<K, V>) element)._1;
    V value = ((Tuple2<K, V>) element)._2;

    keyToValues.putIfAbsent(key, new ArrayList<>());
    keyToValues.get(key).add(value);
  }

  @Override
  public void close() {
    keyToValues.entrySet().stream().map(entry -> new Tuple2<>(entry.getKey(), (Iterable<V>) entry.getValue()))
        .forEach(pipe::emit);
    keyToValues.clear();
  }
}
