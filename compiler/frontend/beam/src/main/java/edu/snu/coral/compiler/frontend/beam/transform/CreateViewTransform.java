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
package edu.snu.coral.compiler.frontend.beam.transform;

import edu.snu.coral.common.ir.Pipe;
import edu.snu.coral.common.ir.vertex.transform.Transform;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * CreateView transform implementation.
 * @param <I> input type.
 * @param <O> output type.
 */
public final class CreateViewTransform<I, O> implements Transform<I, O> {
  private static final Logger LOG = LoggerFactory.getLogger(CreateViewTransform.class.getName());
  private final PCollectionView pCollectionView;
  private final I aggregatedElement;  // This should be the type of CreateViewTransform's I
  private Pipe<O> pipe;
  private List<WindowedValue<I>> windowed;
  private final ViewFn<Iterable<WindowedValue<I>>, O> viewFn;

  /**
   * Constructor of CreateViewTransform.
   * @param pCollectionView the pCollectionView to create.
   */
  public CreateViewTransform(final PCollectionView<O> pCollectionView) {
    this.pCollectionView = pCollectionView;
    this.windowed = new ArrayList<>();
    this.viewFn = this.pCollectionView.getViewFn();

    if (viewFn instanceof PCollectionViews.IterableViewFn
        || viewFn instanceof PCollectionViews.ListViewFn
        || viewFn instanceof PCollectionViews.SingletonViewFn) {
      aggregatedElement = (I) new ArrayList<>();
    } else if (viewFn instanceof PCollectionViews.MapViewFn) {
      aggregatedElement = (I) new ArrayList<>();
    } else {
      // TODO #xxx: Support MultiMap
      throw new UnsupportedOperationException("Unsupported viewFn: " + viewFn.getClass());
    }
  }

  @Override
  public void prepare(final Context context, final Pipe<O> p) {
    this.pipe = p;
  }

  @Override
  public void onData(final Object element) {
    /*
    if (aggregatedElement instanceof ArrayList) {
      ((ArrayList) aggregatedElement).add(element);
    } else if (aggregatedElement instanceof HashMap) {
      final KV kv = (KV) element;
      ((HashMap) aggregatedElement).putIfAbsent(kv.getKey(), kv.getValue());
    }
    */
    windowed.add(WindowedValue.valueInGlobalWindow((I) element));
  }

  /**
   * get the Tag of the Transform.
   * @return the PCollectionView of the transform.
   */
  public PCollectionView getTag() {
    return this.pCollectionView;
  }

  @Override
  public void close() {
    LOG.info("log: pCollectionView {}, viewFn {}", pCollectionView, viewFn);
    pipe.emit(viewFn.apply(windowed));
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("CreateViewTransform:" + pCollectionView);
    return sb.toString();
  }
}
