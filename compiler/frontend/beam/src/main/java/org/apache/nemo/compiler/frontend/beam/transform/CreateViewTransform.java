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
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.values.KV;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * This transform receives data and forwards them to the group by key and window transform.
 *
 * Dataflow:
 * data -- CreateViewTransform.onData() -- GBKWTransform.onData() -- GBKWOutputCollectorWrapper.emit()
 *
 * watermark -- CReateViewTransform.onWatermark()
 *                            -- GBKWTransform.onWatermark() -- GBKWOutputCollectorWrapper.emitWatermark()
 * @param <I> input type.
 * @param <O> output type.
 */
public final class CreateViewTransform<I, O> implements
  Transform<WindowedValue<KV<?, I>>, WindowedValue<O>> {
  private OutputCollector<WindowedValue<O>> outputCollector;
  private final ViewFn<Materializations.MultimapView<Void, ?>, O> viewFn;
  private final Transform<WindowedValue<KV<?, I>>, WindowedValue<KV<?, Iterable<I>>>> gbkwTransform;

  /**
   * Constructor of CreateViewTransform.
   * @param viewFn the viewFn that materializes data.
   * @param gbkwTransform group by window transform (single key)
   */
  public CreateViewTransform(
    final ViewFn<Materializations.MultimapView<Void, ?>, O> viewFn,
    final Transform<WindowedValue<KV<?, I>>, WindowedValue<KV<?, Iterable<I>>>> gbkwTransform) {
    this.viewFn = viewFn;
    this.gbkwTransform = gbkwTransform;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<WindowedValue<O>> oc) {
    this.outputCollector = oc;
    gbkwTransform.prepare(context, new GBKWOutputCollectorWrapper());
  }

  @Override
  public void onData(final WindowedValue<KV<?, I>> element) {
    // The key is always null in CreateViewTransform
    // because Beam translates the key to null.
    // Therefore, the group by key and window is performed on a single (null) key
    gbkwTransform.onData(element);
  }

  @Override
  public void onWatermark(final Watermark watermark) {
    gbkwTransform.onWatermark(watermark);
  }

  @Override
  public void close() {
    gbkwTransform.close();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("CreateViewTransform:" + viewFn);
    return sb.toString();
  }

  /**
   * Represents {@code PrimitiveViewT} supplied to the {@link ViewFn}.
   * @param <T> primitive view type
   */
  public final class MultiView<T> implements Materializations.MultimapView<Void, T>, Serializable {
    private final Iterable<T> iterable;

    /**
     * Constructor.
     */
    MultiView(final Iterable<T> iterable) {
      // Create a placeholder for side input data. CreateViewTransform#onData stores data to this list.
      this.iterable = iterable;
    }

    @Override
    public Iterable<T> get(@Nullable final Void aVoid) {
      return iterable;
    }
  }

  /**
   * This is a output collector wrapper that handles emitted data and watermark from GBKWTransform.
   */
  final class GBKWOutputCollectorWrapper implements OutputCollector<WindowedValue<KV<?, Iterable<I>>>> {

    @Override
    public void emit(final WindowedValue<KV<?, Iterable<I>>> output) {
      final O view = viewFn.apply(new MultiView<>(output.getValue().getValue()));
      System.out.println("Emit: " + view);
      outputCollector.emit(output.withValue(view));
    }

    @Override
    public void emitWatermark(final Watermark watermark) {
      outputCollector.emitWatermark(watermark);
    }

    @Override
    public <T> void emit(final String dstVertexId, final T output) {
      outputCollector.emit(dstVertexId, output);
    }
  }
}
