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
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.SideInputElement;

/**
 * Side input transform implementation.
 * TODO #297: Consider Removing SideInputTransform
 * @param <T> input/output type.
 */
public final class SideInputTransform<T> implements Transform<WindowedValue<T>, WindowedValue<SideInputElement<T>>> {
  private OutputCollector<WindowedValue<SideInputElement<T>>> outputCollector;
  private final int index;

  /**
   * Constructor.
   * @param index side input index.
   */
  public SideInputTransform(final int index) {
    this.index = index;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<WindowedValue<SideInputElement<T>>> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final WindowedValue<T> element) {
    outputCollector.emit(element.withValue(new SideInputElement<>(index, element.getValue())));
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
    sb.append("SideInputTransform:");
    sb.append("(index-");
    sb.append(String.valueOf(index));
    sb.append(")");
    sb.append(super.toString());
    return sb.toString();
  }
}
