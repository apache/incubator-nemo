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
package edu.snu.nemo.common.ir.vertex.transform;

import edu.snu.nemo.common.ir.OutputCollector;

/**
 * A {@link Transform} relays input data from upstream vertex to downstream vertex promptly.
 * This transform can be used for merging input data into the {@link OutputCollector}.
 * @param <T> input/output type.
 */
public final class RelayTransform<T> implements Transform<T, T> {
  private OutputCollector<T> outputCollector;

  /**
   * Default constructor.
   */
  public RelayTransform() {
    // Do nothing.
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> p) {
    this.outputCollector = p;
  }

  @Override
  public void onData(final Object element) {
    outputCollector.emit((T) element);
  }

  @Override
  public void close() {
    // Do nothing.
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(RelayTransform.class);
    sb.append(":");
    sb.append(super.toString());
    return sb.toString();
  }
}
