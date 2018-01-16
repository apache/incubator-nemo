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
import edu.snu.onyx.common.ir.vertex.transform.Transform;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Broadcast transform implementation.
 * @param <I> input type.
 * @param <O> output type.
 */
public final class BroadcastTransform<I, O> implements Transform<I, O> {
  private static final Logger LOG = LoggerFactory.getLogger(BroadcastTransform.class.getName());

  private final PCollectionView pCollectionView;
  private OutputCollector<O> outputCollector;

  /**
   * Constructor of BroadcastTransform.
   * @param pCollectionView the pCollectionView to broadcast.
   */
  public BroadcastTransform(final PCollectionView<O> pCollectionView) {
    this.pCollectionView = pCollectionView;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<O> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final Object element) {
    LOG.info("log_bc: element {}", element);
    final List<WindowedValue<I>> windowed = new ArrayList<>();
    windowed.add(WindowedValue.valueInGlobalWindow((I) element));
    //LOG.info("log_bc: windowed {}", windowed);

    final ViewFn<Iterable<WindowedValue<I>>, O> viewFn = this.pCollectionView.getViewFn();
    Object output = viewFn.apply(windowed);
    LOG.info("log_bc: emitting {}", output);
    outputCollector.emit((O) output);
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
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("BroadcastTransform:" + pCollectionView);
    return sb.toString();
  }
}
