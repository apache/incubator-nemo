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
package org.apache.nemo.compiler.frontend.beam;

import org.apache.beam.runners.core.ReadyCheckingSideInputReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.nemo.common.Pair;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

/**
 */
public final class MaterializedViewReader<T> extends ReadyCheckingSideInputReader {
  final Set<PCollectionView<?>> sideInputs;
  final Map<Pair<PCollectionView<?>, BoundedWindow>, Object> materializedViews;

  public MaterializedViewReader() {
  }

  @Override
  public boolean isReady(final PCollectionView<?> view, final BoundedWindow window) {
    return materializedViews.containsKey(Pair.<>of(view, window));
  }

  @Nullable
  @Override
  public <T> T get(final PCollectionView<T> view, final BoundedWindow window) {
    return materializedViews.get(Pair.of(view, window));
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return sideInputs.contains(view);
  }

  @Override
  public boolean isEmpty() {
    return sideInputs.isEmpty();
  }

  public void addView(final PCollectionView<T> view, final BoundedWindow window, final WindowedValue materializedData) {
    materializedViews.put(Pair.of(view, window), materializedData);
  }
}
