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
package edu.snu.onyx.common.ir.vertex;

import edu.snu.onyx.common.ir.Readable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.snu.onyx.common.ir.ReadablesWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SourceVertex implementation for BoundedSource.
 * @param <O> output type.
 */
public final class BoundedSourceVertex<O> extends SourceVertex<O> {
  private static final Logger LOG = LoggerFactory.getLogger(BoundedSourceVertex.class.getName());
  private final Source<O> source;

  /**
   * Constructor of BoundedSourceVertex.
   * @param source BoundedSource to read from.
   */
  public BoundedSourceVertex(final Source<O> source) {
    this.source = source;
  }

  @Override
  public BoundedSourceVertex getClone() {
    final BoundedSourceVertex that = new BoundedSourceVertex<>(this.source);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public ReadablesWrapper<O> getReadableWrapper(final int desiredNumOfSplits) throws Exception {
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
      LOG.info("estimate: {}", source.getEstimatedSizeBytes());
      LOG.info("desired: {}", desiredNumOfSplits);
      source.split(source.getEstimatedSizeBytes() / desiredNumOfSplits).forEach(boundedSource ->
          readables.add(new BoundedSourceReadable<>(boundedSource)));
      return readables;
    }
  }

  /**
   * BoundedSourceReadable class.
   * @param <T> type.
   */
  private final class BoundedSourceReadable<T> implements Readable<T> {
    private final Source<T> boundedSource;

    /**
     * Constructor of the BoundedSourceReadable.
     * @param boundedSource the BoundedSource.
     */
    BoundedSourceReadable(final Source<T> boundedSource) {
      this.boundedSource = boundedSource;
    }

    @Override
    public Iterator<T> read() throws Exception {
      return new BoundedSourceIterator<>(boundedSource.createReader());
    }
  }

  /**
   * Iterator for the bounded source reader.
   * @param <T> type of the data.
   */
  private final class BoundedSourceIterator<T> implements Iterator<T> {
    private final Source.Reader<T> reader;
    private boolean available;

    /**
     * Constructor.
     * @param reader reader to read from.
     * @throws Exception exceptions.
     */
    private BoundedSourceIterator(final Source.Reader<T> reader) throws Exception {
      this.reader = reader;
      this.available = reader.start();
    }

    @Override
    public boolean hasNext() {
      return available;
    }

    @Override
    public T next() {
      final T value = reader.getCurrent();
      try {
        available = reader.advance();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return value;
    }
  }
}
