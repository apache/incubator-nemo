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
  }

  @Override
  public void prepare(final Context context, final Pipe<O> p) {
    this.pipe = p;
  }

  @Override
  public void onData(final Object element) {
    WindowedValue<I> data = WindowedValue.valueInGlobalWindow((I) element);
    windowed.add(data);
    LOG.info("CreateViewTransform onData {}", data);
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
    O output = viewFn.apply(windowed);
    pipe.emit(output);
    LOG.info("CreateViewTransform close, emitting {}", output);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("CreateViewTransform:" + pCollectionView);
    return sb.toString();
  }
}
