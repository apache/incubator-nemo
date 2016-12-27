/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.vortex.translator.beam.node;

import edu.snu.vortex.compiler.plan.node.Do;
import edu.snu.vortex.translator.beam.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.ArrayList;
import java.util.Map;

public class DoImpl<I, O> extends Do<I, O, PCollectionView> {
  private final DoFn doFn;

  public DoImpl(final DoFn doFn) {
    this.doFn = doFn;
  }

  @Override
  public Iterable<O> transform(final Iterable<I> input, final Map<PCollectionView, Object> broadcasted) {
    final DoFnInvoker<I, O> invoker = DoFnInvokers.invokerFor(doFn);
    final ArrayList<O> outputList = new ArrayList<>();
    final ProcessContext<I, O> context = new ProcessContext<>(doFn, outputList, broadcasted);
    invoker.invokeSetup();
    invoker.invokeStartBundle(context);
    input.forEach(element -> {
      context.setElement(element);
      invoker.invokeProcessElement(context);
    });
    invoker.invokeFinishBundle(context);
    invoker.invokeTeardown();
    return outputList;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append(", doFn: ");
    sb.append(doFn);
    return sb.toString();
  }
}

