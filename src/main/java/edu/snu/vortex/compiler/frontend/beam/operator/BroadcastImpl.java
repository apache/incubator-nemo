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
package edu.snu.vortex.compiler.frontend.beam.operator;

import edu.snu.vortex.compiler.ir.operator.Broadcast;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BroadcastImpl<I, O> extends Broadcast<I, O, PCollectionView> {
  private final PCollectionView view;

  public BroadcastImpl(final PCollectionView<O> view) {
    this.view = view;
  }

  @Override
  public O transform(final Iterable<I> inputs) {
    final List<WindowedValue<I>> windowed = StreamSupport.stream(inputs.spliterator(), false)
        .map(input -> WindowedValue.valueInGlobalWindow(input)) // We only support batch for now
        .collect(Collectors.toList());
    final ViewFn<Iterable<WindowedValue<I>>, O> viewFn = view.getViewFn();
    return viewFn.apply(windowed);
  }

  @Override
  public PCollectionView getTag() {
    return view;
  }
}
