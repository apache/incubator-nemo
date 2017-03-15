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
package edu.snu.vortex.compiler.frontend.beam.transform;

import edu.snu.vortex.compiler.frontend.beam.BeamElement;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.compiler.ir.OutputCollector;
import edu.snu.vortex.compiler.ir.Transform;
import org.apache.beam.sdk.values.KV;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Group Beam KVs.
 */
public final class GroupByKeyTransform implements Transform {
  private final Map<Object, List> keyToValues;
  private OutputCollector outputCollector;

  public GroupByKeyTransform() {
    this.keyToValues = new HashMap<>();
  }

  @Override
  public void prepare(final Context context, final OutputCollector oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final Iterable<Element> data, final String srcVertexId) {
    data.forEach(element -> {
      final KV kv = (KV) element.getData();
      keyToValues.putIfAbsent(kv.getKey(), new ArrayList());
      keyToValues.get(kv.getKey()).add(kv.getValue());
    });
  }

  @Override
  public void close() {
    keyToValues.entrySet().stream()
        .map(entry -> KV.of(entry.getKey(), entry.getValue()))
        .forEach(wv -> outputCollector.emit(new BeamElement<>(wv)));
    keyToValues.clear();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("GroupByKeyTransform");
    return sb.toString();
  }
}

