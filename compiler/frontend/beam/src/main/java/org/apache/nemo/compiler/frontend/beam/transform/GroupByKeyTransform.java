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

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.NoWatermarkEmitTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Group Beam KVs.
 *
 * @param <I> input type.
 */
public final class GroupByKeyTransform<I> extends NoWatermarkEmitTransform<I, WindowedValue<KV<Object, List>>> {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByKeyTransform.class.getName());
  private final Map<Object, List> keyToValues;
  private OutputCollector<WindowedValue<KV<Object, List>>> outputCollector;

  /**
   * GroupByKey constructor.
   */
  public GroupByKeyTransform() {
    this.keyToValues = new HashMap<>();
  }

  @Override
  public void prepare(final Context context, final OutputCollector<WindowedValue<KV<Object, List>>> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final I element) {
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
      final Iterator<Map.Entry<Object, List>> iterator = keyToValues.entrySet().iterator();
      while (iterator.hasNext()) {
        final Map.Entry<Object, List> entry = iterator.next();
        outputCollector.emit(WindowedValue.valueInGlobalWindow(KV.of(entry.getKey(), entry.getValue())));
        iterator.remove();
      }
    }
  }
}
