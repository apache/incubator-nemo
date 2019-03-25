/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.compiler.frontend.spark.transform;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.NoWatermarkEmitTransform;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transform for group by key transformation.
 *
 * @param <K> key type.
 * @param <V> value type.
 */
public final class GroupByKeyTransform<K, V> extends NoWatermarkEmitTransform<Tuple2<K, V>, Tuple2<K, Iterable<V>>> {
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
