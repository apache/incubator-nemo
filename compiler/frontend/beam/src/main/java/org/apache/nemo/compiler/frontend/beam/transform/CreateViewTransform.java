/*
 * Copyright (C) 2018 Seoul National University
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
package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * CreateView transform implementation.
 * @param <I> input type.
 * @param <O> output type.
 */
public final class CreateViewTransform<I, O> implements Transform<WindowedValue<I>, WindowedValue<O>> {
  private final PCollectionView pCollectionView;
  private OutputCollector<WindowedValue<O>> outputCollector;
  private final ViewFn<Materializations.MultimapView<Void, ?>, O> viewFn;
  private final MultiView<Object> multiView;

  /**
   * Constructor of CreateViewTransform.
   * @param pCollectionView the pCollectionView to create.
   */
  public CreateViewTransform(final PCollectionView<O> pCollectionView) {
    this.pCollectionView = pCollectionView;
    this.viewFn = this.pCollectionView.getViewFn();
    this.multiView = new MultiView<>();
  }

  @Override
  public void prepare(final Context context, final OutputCollector<WindowedValue<O>> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final WindowedValue<I> element) {
    // Since CreateViewTransform takes KV(Void, value), this is okay
    /*
    if (element instanceof KV) {
      final KV<?, ?> kv = (KV<?, ?>) element;
      multiView.getDataList().add(kv.getValue());
    }
    */
    // TODO #: support window in view
    final KV kv = ((WindowedValue<KV>) element).getValue();
    multiView.getDataList().add(kv.getValue());
  }

  @Override
  public void close() {
    final Object view = viewFn.apply(multiView);
    // TODO #: support window in view
    outputCollector.emit(WindowedValue.valueInGlobalWindow((O) view));
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("CreateViewTransform:" + pCollectionView);
    return sb.toString();
  }

  /**
   * Represents {@code PrimitiveViewT} supplied to the {@link ViewFn}.
   * @param <T> primitive view type
   */
  public final class MultiView<T> implements Materializations.MultimapView<Void, T>, Serializable {
    private final ArrayList<T> dataList;

    /**
     * Constructor.
     */
    MultiView() {
      // Create a placeholder for side input data. CreateViewTransform#onData stores data to this list.
      dataList = new ArrayList<>();
    }

    @Override
    public Iterable<T> get(@Nullable final Void aVoid) {
      return dataList;
    }

    public ArrayList<T> getDataList() {
      return dataList;
    }
  }
}
