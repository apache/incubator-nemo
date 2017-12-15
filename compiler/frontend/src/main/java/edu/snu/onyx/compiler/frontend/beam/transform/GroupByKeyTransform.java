/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.onyx.compiler.frontend.beam.transform;

import edu.snu.onyx.common.ir.OutputCollector;
import edu.snu.onyx.common.ir.vertex.transform.Transform;
import org.apache.beam.sdk.values.KV;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Group Beam KVs.
 * @param <I> input type.
 */
public final class GroupByKeyTransform<I> implements Transform<I, KV<Object, List>> {
  private final Map<Object, List> keyToValues;
  private OutputCollector<KV<Object, List>> outputCollector;

  /**
   * GroupByKey constructor.
   */
  public GroupByKeyTransform() {
    this.keyToValues = new HashMap<>();
  }

  @Override
  public void prepare(final Context context, final OutputCollector<KV<Object, List>> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final I element) {
    final KV kv = (KV) element;
    keyToValues.putIfAbsent(kv.getKey(), new ArrayList());
    keyToValues.get(kv.getKey()).add(kv.getValue());
  }

  @Override
  public void close() {
    keyToValues.entrySet().stream().map(entry -> KV.of(entry.getKey(), entry.getValue()))
        .forEach(wv -> outputCollector.emit(wv));
    keyToValues.clear();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("GroupByKeyTransform:");
    sb.append(super.toString());
    return sb.toString();
  }
}

