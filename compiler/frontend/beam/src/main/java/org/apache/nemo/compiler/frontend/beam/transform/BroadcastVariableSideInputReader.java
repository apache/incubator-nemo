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

import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.nemo.common.ir.vertex.transform.Transform;

import javax.annotation.Nullable;
import java.util.Collection;

/**
 * A sideinput reader that reads/writes side input values to context.
 */
public final class BroadcastVariableSideInputReader implements SideInputReader {

  // Nemo context for storing/getting side inputs
  private final Transform.Context context;

  // The list of side inputs that we're handling
  private final Collection<PCollectionView<?>> sideInputs;

  BroadcastVariableSideInputReader(final Transform.Context context,
                                   final Collection<PCollectionView<?>> sideInputs) {
    this.context = context;
    this.sideInputs = sideInputs;
  }

  @Nullable
  @Override
  public <T> T get(final PCollectionView<T> view, final BoundedWindow window) {
    // TODO #216: implement side input and windowing
    return ((WindowedValue<T>) context.getBroadcastVariable(view)).getValue();
  }

  @Override
  public <T> boolean contains(final PCollectionView<T> view) {
    return sideInputs.contains(view);
  }

  @Override
  public boolean isEmpty() {
    return sideInputs.isEmpty();
  }
}
