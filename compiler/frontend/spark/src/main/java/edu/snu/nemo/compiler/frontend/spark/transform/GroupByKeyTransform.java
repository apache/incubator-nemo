/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
