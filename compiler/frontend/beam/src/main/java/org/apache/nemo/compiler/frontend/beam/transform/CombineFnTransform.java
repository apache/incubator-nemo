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
package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.runners.core.GlobalCombineFnRunner;
import org.apache.beam.runners.core.GlobalCombineFnRunners;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.NoWatermarkEmitTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @param <I> Input type.
 * @param <A> Accum type.
 */
public final class CombineFnTransform<K, I, A>
  extends NoWatermarkEmitTransform<WindowedValue<KV<K, I>>, WindowedValue<KV<K, I>>> {
  private static final Logger LOG = LoggerFactory.getLogger(CombineFnTransform.class.getName());
  private final Map<Object, List> keyToValues;
  private OutputCollector<WindowedValue<KV<Object, List>>> outputCollector;


  private final GlobalCombineFnRunner<I, A, ?> combineFnRunner;

  /**
   * GroupByKey constructor.
   */
  public CombineFnTransform(final CombineFnBase.GlobalCombineFn<I, A, ?> combineFn) {
    this.combineFnRunner = GlobalCombineFnRunners.create(combineFn);
  }

  @Override
  public void prepare(final Context context, final OutputCollector<WindowedValue<KV<K, I>>> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final WindowedValue<KV<K, I>> element) {
    // Windowing here (?)
    combineFnRunner.

    combineFn.mergeAccumulators()


    final WindowedValue<KV> windowedValue = (WindowedValue<KV>) element;
    final KV kv = windowedValue.getValue();
    keyToValues.putIfAbsent(kv.getKey(), new ArrayList());
    keyToValues.get(kv.getKey()).add(kv.getValue());
  }

  @Override
  public void close() {
    if (keyToValues.isEmpty()) {
      LOG.warn("Beam GroupByKeyTransform received no data!");
    } else {
      keyToValues.entrySet().stream().map(entry ->
        WindowedValue.valueInGlobalWindow(KV.of(entry.getKey(), entry.getValue())))
        .forEach(outputCollector::emit);
      keyToValues.clear();
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("GroupByKeyTransform:");
    sb.append(super.toString());
    return sb.toString();
  }
}
