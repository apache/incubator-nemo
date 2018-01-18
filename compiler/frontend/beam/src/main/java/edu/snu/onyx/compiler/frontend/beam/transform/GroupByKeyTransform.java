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

import edu.snu.onyx.common.ir.Pipe;
import edu.snu.onyx.common.ir.vertex.transform.Transform;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Group Beam KVs.
 * @param <I> input type.
 */
public final class GroupByKeyTransform<I> implements Transform<I, KV<Object, List>> {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByKeyTransform.class.getName());

  private final Map<Object, List> keyToValues;
  private Pipe<KV<Object, List>> pipe;

  /**
   * GroupByKey constructor.
   */
  public GroupByKeyTransform() {
    this.keyToValues = new HashMap<>();
  }

  @Override
  public void prepare(final Context context, final Pipe<KV<Object, List>> p) {
    this.pipe = p;
  }

  @Override
  public void onData(final Object element) {
    final KV kv = (KV) element;
    keyToValues.putIfAbsent(kv.getKey(), new ArrayList());
    keyToValues.get(kv.getKey()).add(kv.getValue());

    //keyToValues.entrySet().stream().map(entry2 -> KV.of(entry2.getKey(), entry2.getValue()))
    //    .forEach(wv -> System.out.println(String.format("log_gbk: onData(): keyToValues: %s %s",
    //        wv.getKey(), wv.getValue())));
  }

  @Override
  public void close() {
    keyToValues.entrySet().stream().map(entry -> KV.of(entry.getKey(), entry.getValue()))
        .forEach(wv -> {
          pipe.emit(wv);
        });
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

