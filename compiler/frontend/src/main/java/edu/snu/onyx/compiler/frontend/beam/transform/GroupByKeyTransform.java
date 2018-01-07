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
import edu.snu.onyx.common.ir.Transform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Group Beam KVs.
 * @param <I> input type.
 */
public final class GroupByKeyTransform<I> implements Transform<WindowedValue<I>, WindowedValue<KV<Object, List>>> {
  private final Map<BoundedWindow, Map<Object, List>> kwToDataMap;
  private OutputCollector<WindowedValue<KV<Object, List>>> outputCollector;

  /**
   * GroupByKey constructor.
   */
  public GroupByKeyTransform() {
    this.kwToDataMap = new HashMap<>();
  }

  @Override
  public void prepare(final Context context, final OutputCollector<WindowedValue<KV<Object, List>>> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final WindowedValue<I> element) {
    final BoundedWindow window = element.getWindows().iterator().next();
    kwToDataMap.putIfAbsent(window, new HashMap<>());
    final KV kv = (KV) ((WindowedValue) element).getValue();
    final Map<Object, List> keyToValues = kwToDataMap.get(window);
    keyToValues.putIfAbsent(kv.getKey(), new ArrayList());
    keyToValues.get(kv.getKey()).add(kv.getValue());

    kwToDataMap.entrySet().stream().forEach(entry -> {
      final BoundedWindow boundedwindow = entry.getKey();
      System.out.println(String.format("log_gbk: window: %s", boundedwindow));
      final  Map<Object, List> keyToValues2 = entry.getValue();
      keyToValues2.entrySet().stream().map(entry2 -> KV.of(entry2.getKey(), entry2.getValue()))
          .forEach(wv -> System.out.println(String.format("log_gbk: keyToValues: %s %s", wv.getKey(), wv.getValue())));
      });
  }

  @Override
  public void close() {
    AtomicInteger entryNum = new AtomicInteger(0);
    kwToDataMap.entrySet().stream().forEach(windowEntry -> {
      System.out.println(String.format("log_gbk: close() entry %d", entryNum.getAndIncrement()));
      final BoundedWindow window = windowEntry.getKey();
      final  Map<Object, List> keyToValues = windowEntry.getValue();

      keyToValues.entrySet().stream().map(entry -> KV.of(entry.getKey(), entry.getValue()))
          .forEach(wv -> {
            System.out.println(String.format("log_gbk: close() emitting %s", wv));
              outputCollector
              .emit(WindowedValue.of(wv,
                  window.maxTimestamp(),
                  window,
                  PaneInfo.ON_TIME_AND_ONLY_FIRING));
              }
          );
      keyToValues.clear();
    });
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("GroupByKeyTransform:");
    sb.append(super.toString());
    return sb.toString();
  }
}
