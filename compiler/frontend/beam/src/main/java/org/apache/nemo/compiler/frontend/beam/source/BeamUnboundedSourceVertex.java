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
package org.apache.nemo.compiler.frontend.beam.source;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.test.EmptyComponents;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * SourceVertex implementation for UnboundedSource.
 *
 * @param <O> output type.
 * @param <M> checkpoint mark type.
 */
public final class BeamUnboundedSourceVertex<O, M extends UnboundedSource.CheckpointMark> extends
  SourceVertex<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(BeamUnboundedSourceVertex.class.getName());
  private UnboundedSource<O, M> source;
  private final DisplayData displayData;

  /**
   * The default constructor for beam unbounded source.
   *
   * @param source      unbounded source.
   * @param displayData static display data associated with a pipeline component.
   */
  public BeamUnboundedSourceVertex(final UnboundedSource<O, M> source,
                                   final DisplayData displayData) {
    this.source = source;
    this.displayData = displayData;
  }

  /**
   * Copy constructor.
   *
   * @param that the original vertex.
   */
  private BeamUnboundedSourceVertex(final BeamUnboundedSourceVertex<O, M> that) {
    super(that);
    this.source = that.source;
    this.displayData = that.displayData;
  }

  @Override
  public IRVertex getClone() {
    return new BeamUnboundedSourceVertex<>(this);
  }

  @Override
  public boolean isBounded() {
    return false;
  }

  @Override
  public List<Readable<Object>> getReadables(final int desiredNumOfSplits) throws Exception {

    final List<Readable<Object>> readables = new ArrayList<>();
    if (source != null) {
      source.split(desiredNumOfSplits, null)
        .forEach(unboundedSource -> readables.add(new UnboundedSourceReadable<>(unboundedSource)));
      return readables;
    } else {
      // TODO #333: Remove SourceVertex#clearInternalStates
      final SourceVertex emptySourceVertex = new EmptyComponents.EmptySourceVertex("EMPTY");
      return emptySourceVertex.getReadables(desiredNumOfSplits);
    }
  }

  @Override
  public String getSourceName() {
    return this.source.getClass().getSimpleName();
  }

  @Override
  public long getEstimatedSizeBytes() {
    return 0L;
  }

  @Override
  public void clearInternalStates() {
    source = null;
  }

  @Override
  public ObjectNode getPropertiesAsJsonNode() {
    final ObjectNode node = getIRVertexPropertiesAsJsonNode();
    node.put("source", displayData.toString());
    return node;
  }

  /**
   * UnboundedSourceReadable class.
   *
   * @param <O> output type.
   * @param <M> checkpoint mark type.
   */
  private static final class UnboundedSourceReadable<O, M extends UnboundedSource.CheckpointMark>
    implements Readable<Object> {
    private final UnboundedSource<O, M> unboundedSource;
    private UnboundedSource.UnboundedReader<O> reader;
    private boolean isStarted = false;
    private boolean isCurrentAvailable = false;
    private boolean isFinished = false;

    /**
     * Constructor.
     *
     * @param unboundedSource unbounded source.
     */
    UnboundedSourceReadable(final UnboundedSource<O, M> unboundedSource) {
      this.unboundedSource = unboundedSource;
    }

    @Override
    public void prepare() {
      try {
        reader = unboundedSource.createReader(null, null);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Object readCurrent() {
      try {
        if (!isStarted) {
          isStarted = true;
          isCurrentAvailable = reader.start();
        } else {
          isCurrentAvailable = reader.advance();
        }
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }

      if (isCurrentAvailable) {
        final O elem = reader.getCurrent();
        return WindowedValue.timestampedValueInGlobalWindow(elem, reader.getCurrentTimestamp());
      } else {
        throw new NoSuchElementException();
      }
    }

    @Override
    public long readWatermark() {
      final Instant watermark = reader.getWatermark();
      // Finish if the watermark == TIMESTAMP_MAX_VALUE
      isFinished = (watermark.getMillis() >= GlobalWindow.TIMESTAMP_MAX_VALUE.getMillis());
      return watermark.getMillis();
    }

    @Override
    public boolean isFinished() {
      return isFinished;
    }

    @Override
    public List<String> getLocations() throws Exception {
      return new ArrayList<>();
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
