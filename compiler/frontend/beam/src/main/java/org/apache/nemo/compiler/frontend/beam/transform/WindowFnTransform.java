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

import com.google.common.collect.Iterables;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.nemo.common.punctuation.Watermark;
import org.joda.time.Instant;

import java.util.Collection;

/**
 * Windowing transform implementation.
 * This transform simply windows the given elements into
 * finite windows according to a user-specified WindowFnTransform.
 * @param <T> input/output type.
 * @param <W> window type
 */
public final class WindowFnTransform<T, W extends BoundedWindow>
  implements Transform<WindowedValue<T>, WindowedValue<T>> {
  private final WindowFn windowFn;
  private final DisplayData displayData;
  private OutputCollector<WindowedValue<T>> outputCollector;

  /**
   * Default Constructor.
   * @param windowFn windowFn for the Transform.
   * @param displayData display data.
   */
  public WindowFnTransform(final WindowFn windowFn, final DisplayData displayData) {
    this.windowFn = windowFn;
    this.displayData = displayData;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<WindowedValue<T>> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final WindowedValue<T> windowedValue) {
    final BoundedWindow boundedWindow = Iterables.getOnlyElement(windowedValue.getWindows());
    final T element = windowedValue.getValue();
    final Instant timestamp = windowedValue.getTimestamp();

    try {
      final Collection<W> windows =
        ((WindowFn<T, W>) windowFn)
          .assignWindows(
            ((WindowFn<T, W>) windowFn).new AssignContext() {
              @Override
              public T element() {
                return element;
              }

              @Override
              public Instant timestamp() {
                return timestamp;
              }

              @Override
              public BoundedWindow window() {
                return boundedWindow;
              }
            });

      // Emit compressed windows for efficiency
      outputCollector.emit(WindowedValue.of(element, timestamp, windows, PaneInfo.NO_FIRING));
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onWatermark(final Watermark watermark) {
    outputCollector.emitWatermark(watermark);
  }

  @Override
  public void close() {
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("WindowFnTransform / " + displayData.toString().replaceAll(":", " / "));
    return sb.toString();
  }
}
