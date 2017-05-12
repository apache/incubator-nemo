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
package edu.snu.vortex.compiler.frontend.beam.transform;

import edu.snu.vortex.compiler.frontend.beam.BeamElement;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.compiler.ir.OutputCollector;
import edu.snu.vortex.compiler.ir.Transform;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Broadcast transform implementation.
 */
public final class BroadcastTransform implements Transform {
  private final PCollectionView pCollectionView;
  private OutputCollector outputCollector;

  /**
   * Constructor of BroadcastTransform.
   * @param pCollectionView the pCollectionView to broadcast.
   */
  public BroadcastTransform(final PCollectionView pCollectionView) {
    this.pCollectionView = pCollectionView;
  }

  @Override
  public void prepare(final Context context, final OutputCollector oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final Iterable<Element> data, final String srcVertexId) {
    final List<WindowedValue> windowed = StreamSupport.stream(data.spliterator(), false)
        .map(element -> WindowedValue.valueInGlobalWindow(element.getData()))
        .collect(Collectors.toList());
    final ViewFn viewFn = this.pCollectionView.getViewFn();
    outputCollector.emit(new BeamElement<>(viewFn.apply(windowed)));
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
