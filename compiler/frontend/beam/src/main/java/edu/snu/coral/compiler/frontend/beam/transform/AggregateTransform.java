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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionViews;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Aggregate elements to the right type of PCollectionView.
 * @param <I> input type.
 * @param <O> output type.
 */

public final class AggregateTransform<I, O> implements Transform<I, O> {
  private static final Logger LOG = LoggerFactory.getLogger(AggregateTransform.class.getName());
  private final O aggregatedElement;  // This should be the type of CreateViewTransform's I
  private Pipe<O> pipe;
  private final ViewFn<Iterable<WindowedValue<I>>, O> viewFn;

  /**
   * GroupByKey constructor.
   */
  public AggregateTransform(final ViewFn<Iterable<WindowedValue<I>>, O> viewFn) {
    this.viewFn = viewFn;

    if (viewFn instanceof PCollectionViews.IterableViewFn
        || viewFn instanceof PCollectionViews.ListViewFn
        || viewFn instanceof PCollectionViews.SingletonViewFn) {
      aggregatedElement = (O) new ArrayList<>();
    } else if (viewFn instanceof PCollectionViews.MapViewFn) {
      aggregatedElement = (O) new HashMap<>();
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
    if (aggregatedElement instanceof ArrayList) {
      ((ArrayList) aggregatedElement).add(element);
    } else if (aggregatedElement instanceof HashMap) {
      final KV kv = (KV) element;
      ((HashMap) aggregatedElement).putIfAbsent(kv.getKey(), kv.getValue());
    }
  }

  @Override
  public void close() {
    pipe.emit(aggregatedElement);

    if (aggregatedElement instanceof ArrayList) {
      ((ArrayList) aggregatedElement).clear();
    } else if (aggregatedElement instanceof HashMap) {
      ((HashMap) aggregatedElement).clear();
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("AggregateTransform:");
    sb.append(super.toString());
    return sb.toString();
  }
}

