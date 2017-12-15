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

import com.google.common.collect.Iterables;
import edu.snu.onyx.common.ir.OutputCollector;
import edu.snu.onyx.common.ir.Transform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

import java.util.Collection;

/**
 * Windowing transform implementation.
 * This transform simply windows the given elements into finite windows according to a user-specified WindowTransform.
 * As this functionality is unnecessary for batch processing workloads and for Runtime, this is left as below.
 * @param <T> input/output type.
 */
public final class WindowTransform<T> implements Transform<WindowedValue<T>, WindowedValue<T>> {
  private final WindowFn<T, BoundedWindow> windowFn;
  private OutputCollector<WindowedValue<T>> outputCollector;

  /**
   * Default Constructor.
   * @param windowFn windowFn for the Transform.
   */
  public WindowTransform(final WindowFn windowFn) {
    this.windowFn = windowFn;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<WindowedValue<T>> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final Iterable<WindowedValue<T>> elements, final String srcVertexId) {
    // TODO #36: Actually assign windows
    elements.forEach(element -> {
        try {
          final Collection<BoundedWindow> windows = windowFn.assignWindows(windowFn.new AssignContext() {
            @Override
            public T element() {
              return element.getValue();
            }

            @Override
            public Instant timestamp() {
              return element.getTimestamp();
            }

            @Override
            public BoundedWindow window() {
              return Iterables.getOnlyElement(element.getWindows());
            }
          });

          for (BoundedWindow window : windows) {
            outputCollector.emit(WindowedValue.of(
                element.getValue(),
                element.getTimestamp(),
                window,
                element.getPane()));
          }
        } catch (Exception e) {
          throw new RuntimeException();
        }
    });
  }

  @Override
  public void close() {
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("WindowTransform:" + windowFn);
    return sb.toString();
  }
}
