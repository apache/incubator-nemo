package edu.snu.nemo.compiler.frontend.spark.transform;

import edu.snu.nemo.common.ir.OutputCollector;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import scala.Tuple2;

import java.util.*;

/**
 * Transform for group by key transformation.
 * @param <K> key type.
 * @param <V> value type.
 */
public final class GroupByKeyTransform<K, V> implements Transform<Tuple2<K, V>, Tuple2<K, Iterable<V>>> {
  private final Map<K, List<V>> keyToValues;
  private OutputCollector<Tuple2<K, Iterable<V>>> oc;

  /**
   * Constructor.
   */
  public GroupByKeyTransform() {
    this.keyToValues = new HashMap<>();
  }

  @Override
  public void prepare(final Transform.Context context, final OutputCollector<Tuple2<K, Iterable<V>>> outputCollector) {
    this.oc = outputCollector;
  }

  @Override
  public void onData(final Iterator<Tuple2<K, V>> elements, final String srcVertexId) {
    elements.forEachRemaining(element -> {
      keyToValues.putIfAbsent(element._1, new ArrayList<>());
      keyToValues.get(element._1).add(element._2);
    });
  }

  @Override
  public void close() {
    keyToValues.entrySet().stream().map(entry -> new Tuple2<>(entry.getKey(), (Iterable<V>) entry.getValue()))
        .forEach(oc::emit);
    keyToValues.clear();
  }
}
