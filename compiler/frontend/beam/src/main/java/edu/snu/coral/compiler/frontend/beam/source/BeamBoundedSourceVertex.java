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
package edu.snu.coral.compiler.frontend.beam.source;

import edu.snu.coral.common.ir.Readable;

import java.util.ArrayList;
import java.util.List;

import edu.snu.coral.common.ir.ReadablesWrapper;
import edu.snu.coral.common.ir.vertex.SourceVertex;
import org.apache.beam.sdk.io.BoundedSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SourceVertex implementation for BoundedSource.
 * @param <O> output type.
 */
public final class BeamBoundedSourceVertex<O> extends SourceVertex<O> {
  private static final Logger LOG = LoggerFactory.getLogger(BeamBoundedSourceVertex.class.getName());
  private final BoundedSource<O> source;

  /**
   * Constructor of BeamBoundedSourceVertex.
   * @param source BoundedSource to read from.
   */
  public BeamBoundedSourceVertex(final BoundedSource<O> source) {
    this.source = source;
  }

  @Override
  public BeamBoundedSourceVertex getClone() {
    final BeamBoundedSourceVertex that = new BeamBoundedSourceVertex<>(this.source);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public ReadablesWrapper<O> getReadables(final int desiredNumOfSplits) throws Exception {
    return new BoundedSourceReadablesWrapper(desiredNumOfSplits);
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(irVertexPropertiesToString());
    sb.append(", \"source\": \"");
    sb.append(source);
    sb.append("\"}");
    return sb.toString();
  }

  /**
   * A ReadablesWrapper for BoundedSourceVertex.
   */
  private final class BoundedSourceReadablesWrapper implements ReadablesWrapper<O> {
    final int desiredNumOfSplits;

    /**
     * Constructor of the BoundedSourceReadablesWrapper.
     * @param desiredNumOfSplits the number of splits desired.
     */
    private BoundedSourceReadablesWrapper(final int desiredNumOfSplits) {
      this.desiredNumOfSplits = desiredNumOfSplits;
    }

    @Override
    public List<Readable<O>> getReadables() throws Exception {
      final List<Readable<O>> readables = new ArrayList<>();
      LOG.info("estimate: {}", source.getEstimatedSizeBytes(null));
      LOG.info("desired: {}", desiredNumOfSplits);
      source.split(source.getEstimatedSizeBytes(null) / desiredNumOfSplits, null)
          .forEach(boundedSource -> readables.add(new BoundedSourceReadable<>(boundedSource)));
      return readables;
    }
  }

  /**
   * BoundedSourceReadable class.
   * @param <T> type.
   */
  private final class BoundedSourceReadable<T> implements Readable<T> {
    private final BoundedSource<T> boundedSource;

    /**
     * Constructor of the BoundedSourceReadable.
     * @param boundedSource the BoundedSource.
     */
    BoundedSourceReadable(final BoundedSource<T> boundedSource) {
      this.boundedSource = boundedSource;
    }

    @Override
    public Iterable<T> read() throws Exception {
      final ArrayList<T> elements = new ArrayList<>();
      try (BoundedSource.BoundedReader<T> reader = boundedSource.createReader(null)) {
        for (boolean available = reader.start(); available; available = reader.advance()) {
          elements.add(reader.getCurrent());
        }
      }
      return elements;
    }
  }
}
