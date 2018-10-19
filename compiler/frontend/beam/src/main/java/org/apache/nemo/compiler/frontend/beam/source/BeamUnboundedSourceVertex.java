/*
 * Copyright (C) 2018 Seoul National University
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
package org.apache.nemo.compiler.frontend.beam.source;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * SourceVertex implementation for UnboundedSource.
 * @param <O> output type.
 * @param <M> checkpoint mark type.
 */
public final class BeamUnboundedSourceVertex<O, M extends UnboundedSource.CheckpointMark> extends
  SourceVertex<WindowedValue<O>> {

  private static final Logger LOG = LoggerFactory.getLogger(BeamUnboundedSourceVertex.class.getName());
  private UnboundedSource<O, M> source;
  private final String sourceDescription;

  /**
   * The default constructor for beam unbounded source.
   * @param source unbounded source.
   */
  public BeamUnboundedSourceVertex(final UnboundedSource<O, M> source) {
    super();
    this.source = source;
    this.sourceDescription = source.toString();
  }

  private BeamUnboundedSourceVertex(final BeamUnboundedSourceVertex<O, M> that) {
    super(that);
    this.source = that.source;
    this.sourceDescription = that.source.toString();
  }

  @Override
  public IRVertex getClone() {
    return new BeamUnboundedSourceVertex<>(this);
  }

  @Override
  public List<Readable<WindowedValue<O>>> getReadables(final int desiredNumOfSplits) throws Exception {
    final List<Readable<WindowedValue<O>>> readables = new ArrayList<>();
    source.split(desiredNumOfSplits, null)
      .forEach(unboundedSource -> readables.add(new UnboundedSourceReadable<>(unboundedSource)));
    return readables;
  }

  @Override
  public void clearInternalStates() {
    source = null;
  }

  @Override
  public ObjectNode getPropertiesAsJsonNode() {
    final ObjectNode node = getIRVertexPropertiesAsJsonNode();
    node.put("source", sourceDescription);
    return node;
  }

  /**
   * UnboundedSourceReadable class.
   * @param <O> output type.
   * @param <M> checkpoint mark type.
   */
  private static final class UnboundedSourceReadable<O, M extends UnboundedSource.CheckpointMark>
      implements Readable<WindowedValue<O>> {
    private final UnboundedSource<O, M> unboundedSource;

    UnboundedSourceReadable(final UnboundedSource<O, M> unboundedSource) {
      this.unboundedSource = unboundedSource;
    }

    @Override
    public Iterable<WindowedValue<O>> read() throws IOException {
      return new UnboundedSourceIterable<>(unboundedSource);
    }

    @Override
    public List<String> getLocations() throws Exception {
      return null;
    }
  }

  /**
   * The iterable class for unbounded sources.
   * @param <O> output type.
   * @param <M> checkpoint mark type.
   */
  private static final class UnboundedSourceIterable<O, M extends UnboundedSource.CheckpointMark>
      implements Iterable<WindowedValue<O>> {

    private UnboundedSourceIterator<O, M> iterator;

    UnboundedSourceIterable(final UnboundedSource<O, M> unboundedSource) throws IOException {
      this.iterator = new UnboundedSourceIterator<>(unboundedSource);
    }

    @Override
    public Iterator<WindowedValue<O>> iterator() {
      return iterator;
    }
  }

  /**
   * The iterator for unbounded sources.
   * @param <O> output type.
   * @param <M> checkpoint mark type.
   */
  private static final class UnboundedSourceIterator<O, M extends UnboundedSource.CheckpointMark>
      implements Iterator<WindowedValue<O>> {

    private final UnboundedSource.UnboundedReader<O> unboundedReader;
    private boolean available;

    UnboundedSourceIterator(final UnboundedSource<O, M> unboundedSource) throws IOException {
      this.unboundedReader = unboundedSource.createReader(null, null);
      available = unboundedReader.start();
    }

    @Override
    public boolean hasNext() {
      // Unbounded source always has next element until it finishes.
      return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public WindowedValue<O> next() {
      try {
        while (true) {
          if (!available) {
            Thread.sleep(10);
          } else {
            final O element = unboundedReader.getCurrent();
            available = unboundedReader.advance();
            final boolean windowed = element instanceof WindowedValue;
            if (!windowed) {
              return WindowedValue.valueInGlobalWindow(element);
            } else {
              return (WindowedValue<O>) element;
            }
          }
        }
      } catch (final InterruptedException | IOException e) {
        LOG.error("Exception occurred while waiting for the events...");
        e.printStackTrace();
        return null;
      }
    }
  }
}
