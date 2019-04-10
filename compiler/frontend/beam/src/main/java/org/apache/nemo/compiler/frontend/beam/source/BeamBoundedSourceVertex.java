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
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.nemo.common.exception.MetricException;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.test.EmptyComponents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * SourceVertex implementation for BoundedSource.
 *
 * @param <O> output type.
 */
public final class BeamBoundedSourceVertex<O> extends SourceVertex<WindowedValue<O>> {
  private static final Logger LOG = LoggerFactory.getLogger(BeamBoundedSourceVertex.class.getName());
  private BoundedSource<O> source;
  private final DisplayData displayData;
  private final long estimatedSizeBytes;

  /**
   * Constructor of BeamBoundedSourceVertex.
   *
   * @param source      BoundedSource to read from.
   * @param displayData data to display.
   */
  public BeamBoundedSourceVertex(final BoundedSource<O> source, final DisplayData displayData) {
    this.source = source;
    this.displayData = displayData;
    try {
      this.estimatedSizeBytes = source.getEstimatedSizeBytes(null);
    } catch (Exception e) {
      throw new MetricException(e);
    }
  }

  /**
   * Constructor of BeamBoundedSourceVertex.
   *
   * @param that the source object for copying
   */
  private BeamBoundedSourceVertex(final BeamBoundedSourceVertex that) {
    super(that);
    this.source = that.source;
    this.displayData = that.displayData;
    try {
      this.estimatedSizeBytes = source.getEstimatedSizeBytes(null);
    } catch (Exception e) {
      throw new MetricException(e);
    }
  }

  @Override
  public BeamBoundedSourceVertex getClone() {
    return new BeamBoundedSourceVertex(this);
  }

  @Override
  public boolean isBounded() {
    return true;
  }

  @Override
  public List<Readable<WindowedValue<O>>> getReadables(final int desiredNumOfSplits) throws Exception {
    final List<Readable<WindowedValue<O>>> readables = new ArrayList<>();

    if (source != null) {
      LOG.info("estimate: {}", source.getEstimatedSizeBytes(null));
      LOG.info("desired: {}", desiredNumOfSplits);
      source.split(this.estimatedSizeBytes / desiredNumOfSplits, null)
        .forEach(boundedSource -> readables.add(new BoundedSourceReadable<>(boundedSource)));
      return readables;
    } else {
      // TODO #333: Remove SourceVertex#clearInternalStates
      final SourceVertex emptySourceVertex = new EmptyComponents.EmptySourceVertex("EMPTY");
      return emptySourceVertex.getReadables(desiredNumOfSplits);
    }
  }

  @Override
  public String getSourceName() {
    if (this.source != null) {
      return this.source.getClass().getSimpleName();
    } else {
      return "None";
    }
  }

  @Override
  public long getEstimatedSizeBytes() {
    return this.estimatedSizeBytes;
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
   * BoundedSourceReadable class.
   *
   * @param <T> type.
   */
  private static final class BoundedSourceReadable<T> implements Readable<WindowedValue<T>> {
    private final BoundedSource<T> boundedSource;
    private boolean finished = false;
    private BoundedSource.BoundedReader<T> reader;

    /**
     * Constructor of the BoundedSourceReadable.
     *
     * @param boundedSource the BoundedSource.
     */
    BoundedSourceReadable(final BoundedSource<T> boundedSource) {
      this.boundedSource = boundedSource;
    }

    @Override
    public void prepare() {
      try {
        reader = boundedSource.createReader(null);
        finished = !reader.start();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public WindowedValue<T> readCurrent() {
      if (finished) {
        throw new IllegalStateException("Bounded reader read all elements");
      }

      final T elem = reader.getCurrent();

      try {
        finished = !reader.advance();
      } catch (final IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      return WindowedValue.valueInGlobalWindow(elem);
    }

    @Override
    public long readWatermark() {
      throw new UnsupportedOperationException("No watermark");
    }

    @Override
    public boolean isFinished() {
      return finished;
    }

    @Override
    public List<String> getLocations() throws Exception {
      if (boundedSource instanceof HadoopFormatIO.HadoopInputFormatBoundedSource) {
        final Field inputSplitField = boundedSource.getClass().getDeclaredField("inputSplit");
        inputSplitField.setAccessible(true);
        final InputSplit inputSplit = ((HadoopFormatIO.SerializableSplit) inputSplitField
          .get(boundedSource)).getSplit();
        return Arrays.asList(inputSplit.getLocations());
      } else {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public void close() throws IOException {
      finished = true;
      reader.close();
    }
  }
}
