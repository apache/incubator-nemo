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
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * SourceVertex implementation for UnboundedSource.
 * @param <O> output type.
 * @param <M> checkpoint mark type.
 */
public final class BeamUnboundedSourceVertex<O, M extends UnboundedSource.CheckpointMark> extends
  SourceVertex<Object> {

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
  public boolean isBounded() {
    return false;
  }

  @Override
  public List<Readable<Object>> getReadables(final int desiredNumOfSplits) throws Exception {
    final List<Readable<Object>> readables = new ArrayList<>();
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
      implements Readable<Object> {
    private final UnboundedSource<O, M> unboundedSource;
    private UnboundedSource.UnboundedReader<O> reader;
    private boolean isStarted = false;
    private boolean isCurrentAvailable = false;
    private boolean isFinished = false;

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
