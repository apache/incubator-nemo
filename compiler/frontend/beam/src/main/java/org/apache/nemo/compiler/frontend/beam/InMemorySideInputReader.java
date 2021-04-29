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

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.ReadyCheckingSideInputReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.commons.lang.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.compiler.frontend.beam.transform.CreateViewTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Accumulates and provides side inputs in memory.
 */
public final class InMemorySideInputReader implements ReadyCheckingSideInputReader {
  private static final Logger LOG = LoggerFactory.getLogger(InMemorySideInputReader.class.getName());

  private long curWatermark = Long.MIN_VALUE;

  private final Collection<PCollectionView<?>> sideInputsToRead;
  private final WindowingStrategy windowingStrategy;
  private final Map<Pair<PCollectionView<?>, BoundedWindow>, WindowedValue<SideInputElement>> inMemorySideInputs;

  /**
   * @param sideInputsToRead side inputs to read.
   */
  public InMemorySideInputReader(final Collection<PCollectionView<?>> sideInputsToRead,
                                 final WindowingStrategy windowingStrategy) {
    this.sideInputsToRead = sideInputsToRead;
    this.windowingStrategy = windowingStrategy;
    this.inMemorySideInputs = new HashMap<>();
  }

  public void restoreSideInput(final InMemorySideInputReader reader) {
    curWatermark = reader.curWatermark;
    inMemorySideInputs.putAll(reader.inMemorySideInputs);
  }

  private InMemorySideInputReader(final Collection<PCollectionView<?>> sideInputsToRead,
                                 final WindowingStrategy windowingStrategy,
                                 final Coder sideCoder,
                                  final long curWatermark,
                                  final Map<Pair<PCollectionView<?>, BoundedWindow>, WindowedValue<SideInputElement>> inMemorySideInputs) {
    this.sideInputsToRead = sideInputsToRead;
    this.windowingStrategy = windowingStrategy;
    this.inMemorySideInputs = inMemorySideInputs;
    this.curWatermark = curWatermark;
  }

  public void checkpoint(final DataOutputStream dos,
                         final Coder sideCoder) {
    try {
      dos.writeLong(curWatermark);
      dos.writeInt(inMemorySideInputs.size());
      for (final Map.Entry<Pair<PCollectionView<?>, BoundedWindow>, WindowedValue<SideInputElement>> entry :
        inMemorySideInputs.entrySet())  {

        SerializationUtils.serialize(entry.getKey().left(), dos);
        windowingStrategy.getWindowFn().windowCoder().encode(entry.getKey().right(), dos);

        sideCoder.encode(entry.getValue(), dos);

      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static InMemorySideInputReader decode(final InMemorySideInputReader initReader,
                                               final Coder sideCoder,
                                               final DataInputStream dis) {
    try {
      final WindowingStrategy windowingStrategy = initReader.windowingStrategy;
      final long curWatermark = dis.readLong();
      final int size = dis.readInt();
      final Map<Pair<PCollectionView<?>, BoundedWindow>, WindowedValue<SideInputElement>> map = new HashMap<>();
      for (int i = 0; i < size; i++) {
        final PCollectionView<?> view = (PCollectionView<?>) SerializationUtils.deserialize(dis);
        final BoundedWindow window = (BoundedWindow) windowingStrategy.getWindowFn().windowCoder().decode(dis);
        final WindowedValue<SideInputElement> sideInput = (WindowedValue<SideInputElement>) sideCoder.decode(dis);
        map.put(Pair.of(view, window), sideInput);
      }

      return new InMemorySideInputReader(initReader.sideInputsToRead, windowingStrategy, sideCoder, curWatermark, map);

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isReady(final PCollectionView view, final BoundedWindow window) {
    return window.maxTimestamp().getMillis() < curWatermark
      || inMemorySideInputs.containsKey(Pair.of(view, window));
  }

  @Nullable
  @Override
  public <T> T get(final PCollectionView<T> view, final BoundedWindow window) {
    // This gets called after isReady()
    final WindowedValue<SideInputElement> sideInputData =
       inMemorySideInputs.get(Pair.of(view, window));

    return sideInputData == null
      // The upstream gave us an empty sideInput
      ? ((ViewFn<Object, T>) view.getViewFn()).apply(new CreateViewTransform.MultiView<T>(Collections.emptyList()))
      // The upstream gave us a concrete sideInput
      : (T) sideInputData.getValue().getSideInputValue();
  }

  @Override
  public <T> boolean contains(final PCollectionView<T> view) {
    return sideInputsToRead.contains(view);
  }

  @Override
  public boolean isEmpty() {
    return sideInputsToRead.isEmpty();
  }

  /**
   * Stores the side input in memory to be used with main inputs.
   * @param view of the side input.
   * @param sideInputElement to add.
   */
  public void addSideInputElement(final PCollectionView<?> view,
                                  final WindowedValue<SideInputElement> sideInputElement) {
    for (final BoundedWindow bw : sideInputElement.getWindows()) {
      inMemorySideInputs.put(Pair.of(view, bw), sideInputElement);
    }
  }

  /**
   * Say a DoFn of this reader has 3 main inputs and 4 side inputs.
   * Nemo runtime guarantees that the watermark here
   * is the minimum of the all 7 input streams.
   * @param newWatermark to set.
   */
  public void setCurrentWatermarkOfAllMainAndSideInputs(final long newWatermark) {
    if (curWatermark > newWatermark) {
      // Cannot go backwards in time.
      throw new IllegalStateException(curWatermark + " > " + newWatermark);
    }

    this.curWatermark = newWatermark;
    // TODO #282: Handle late data
    inMemorySideInputs.entrySet().removeIf(entry -> {
      return entry.getKey().right().maxTimestamp().getMillis() <= this.curWatermark; // Discard old sideinputs.
    });
  }
}
