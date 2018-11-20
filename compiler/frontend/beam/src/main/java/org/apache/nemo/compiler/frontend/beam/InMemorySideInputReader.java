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
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.nemo.common.Pair;
import org.apache.nemo.compiler.frontend.beam.transform.CreateViewTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Accumulates and provides side inputs in memory.
 * TODO #290: Handle OOMs in InMemorySideInputReader
 */
public final class InMemorySideInputReader implements ReadyCheckingSideInputReader {
  private static final Logger LOG = LoggerFactory.getLogger(InMemorySideInputReader.class.getName());

  private long curWatermark = Long.MIN_VALUE;

  private final Collection<PCollectionView<?>> sideInputsToRead;
  private final Map<Pair<PCollectionView<?>, BoundedWindow>, Object> inMemorySideInputs;


  private final ConcurrentMap<BoundedWindow, Long> windowAccessMap;

  private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  public InMemorySideInputReader(final Collection<PCollectionView<?>> sideInputsToRead) {
    this.sideInputsToRead = sideInputsToRead;
    this.inMemorySideInputs = new HashMap<>();
    this.windowAccessMap = new ConcurrentHashMap<>();

    scheduledExecutorService.scheduleAtFixedRate(() -> {
      windowAccessMap.forEach((window, accessTime) -> {
        System.out.println(window + ", " + "final access time: " + accessTime);
      });
    }, 5, 5, TimeUnit.SECONDS);
  }

  @Override
  public boolean isReady(final PCollectionView view, final BoundedWindow window) {
    windowAccessMap.put(window, System.currentTimeMillis());

    return window.maxTimestamp().getMillis() < curWatermark
      || inMemorySideInputs.containsKey(Pair.of(view, window));
  }

  @Nullable
  @Override
  public <T> T get(final PCollectionView<T> view, final BoundedWindow window) {
    windowAccessMap.put(window, System.currentTimeMillis());
    // This gets called after isReady()
    final T sideInputData = (T) inMemorySideInputs.get(Pair.of(view, window));
    return sideInputData == null
      // The upstream gave us an empty sideInput
      ? ((ViewFn<Object, T>) view.getViewFn()).apply(new CreateViewTransform.MultiView<T>(Collections.emptyList()))
      // The upstream gave us a concrete sideInput
      : sideInputData;
  }

  @Override
  public <T> boolean contains(final PCollectionView<T> view) {
    return sideInputsToRead.contains(view);
  }

  @Override
  public boolean isEmpty() {
    return sideInputsToRead.isEmpty();
  }

  public <T> void addSideInputValue(final PCollectionView<T> view,
                                    final WindowedValue<T> sideInputValue) {
    for (final BoundedWindow bw : sideInputValue.getWindows()) {
      inMemorySideInputs.put(Pair.of(view, bw), sideInputValue.getValue());
    }
  }

  public void trackCurWatermark(final long newWatermark) {
    if (curWatermark > newWatermark) {
      // Cannot go backwards in time.
      throw new IllegalStateException(curWatermark + " > " + newWatermark);
    }
    this.curWatermark = newWatermark;
  }
}
