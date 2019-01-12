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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Accumulates all of the partially accumulated KVs(Key, Accum) into KVs(Key, Output).
 * (Currently supports batch-style global windows only)
 * TODO #263: Partial Combining for Beam Streaming
 * TODO #264: Partial Combining with Beam SideInputs
 * @param <K> Key type.
 * @param <A> Accum type.
 * @param <O> Output type.
 */
public final class CombineFnFinalTransform<K, A, O>
  extends NoWatermarkEmitTransform<WindowedValue<KV<K, A>>, WindowedValue<KV<K, O>>> {
  private static final Logger LOG = LoggerFactory.getLogger(CombineFnFinalTransform.class.getName());
  private final Map<K, A> keyToAcuumulator;
  private OutputCollector<WindowedValue<KV<K, O>>> outputCollector;

  // null arguments when calling methods of this variable, since we don't support sideinputs yet.
  private final GlobalCombineFnRunner<?, A, O> combineFnRunner;

  /**
   * Constructor.
   * @param combineFn combine function.
   */
  public CombineFnFinalTransform(final CombineFnBase.GlobalCombineFn<?, A, O> combineFn) {
    this.combineFnRunner = GlobalCombineFnRunners.create(combineFn);
    this.keyToAcuumulator = new HashMap<>();
  }

  @Override
  public void prepare(final Context context, final OutputCollector<WindowedValue<KV<K, O>>> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final WindowedValue<KV<K, A>> element) {
    final K key = element.getValue().getKey();
    final A accum = element.getValue().getValue();

    // The initial accumulator
    keyToAcuumulator.putIfAbsent(
      key, combineFnRunner.createAccumulator(null, null, null));

    // Get the accumulator
    final A accumulatorForThisElement = keyToAcuumulator.get(key);

    // Update the accumulator (merge)
    keyToAcuumulator.put(
      key,
      combineFnRunner.mergeAccumulators(
        Arrays.asList(accumulatorForThisElement, accum), null, null, null));
  }

  @Override
  public void close() {
    final Iterator<Map.Entry<K, A>> iterator = keyToAcuumulator.entrySet().iterator();
    while (iterator.hasNext()) {
      final Map.Entry<K, A> entry = iterator.next();
      final K key = entry.getKey();
      final A accum = entry.getValue();
      final O output = combineFnRunner.extractOutput(accum, null, null, null);
      outputCollector.emit(WindowedValue.valueInGlobalWindow(KV.of(key, output)));
      iterator.remove(); // for eager garbage collection
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("CombineFnPartialTransform:");
    sb.append(super.toString());
    return sb.toString();
  }
}
