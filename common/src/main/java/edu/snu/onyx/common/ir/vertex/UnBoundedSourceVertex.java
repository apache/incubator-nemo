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

import edu.snu.onyx.common.ir.Reader;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * SourceVertex implementation for UnBoundedSource.
 * @param <O> output type.
 */
public final class UnBoundedSourceVertex<O> extends SourceVertex<O> {
  private static final Logger LOG = LoggerFactory.getLogger(UnBoundedSourceVertex.class.getName());
  private final UnboundedSource<O, ?> source;

  /**
   * Constructor of UnBoundedSourceVertex.
   * @param source UnBoundedSource to read from.
   */
  public UnBoundedSourceVertex(final UnboundedSource<O, ?> source) {
    this.source = source;
  }

  @Override
  public UnBoundedSourceVertex getClone() {
    final UnBoundedSourceVertex that = new UnBoundedSourceVertex<>(this.source);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public List<Reader<O>> getReaders(final int desiredNumOfSplits) throws Exception {
    final List<Reader<O>> readers = new ArrayList<>();
    LOG.info("desired: {}", desiredNumOfSplits);
    source.split(desiredNumOfSplits, null).forEach(splittedSource -> {
      readers.add(new UnBoundedReader(splittedSource));
    });

    for (final UnboundedSource<O, ?> s : source.split((int) desiredNumOfSplits, null)) {
      readers.add(new UnBoundedReader(s));
    }
    return readers;
  }

  /**
   * UnBoundedSourceReader class.
   * @param <T> type.
   */
  public final class UnBoundedReader<T> implements Reader<WindowedValue<T>> {
    private final UnboundedSource<T, ?> unBoundedSource;
    private UnboundedSource.UnboundedReader<T> reader;
    private boolean firstRead;

    /**
     * Constructor of the UnBoundedSourceReader.
     * @param unBoundedSource the UnBoundedSource.
     */
    UnBoundedReader(final UnboundedSource<T, ?> unBoundedSource) {
      this.unBoundedSource = unBoundedSource;
      this.firstRead = true;
    }

    @Override
    public Iterable<WindowedValue<T>> read() throws Exception {
      final ArrayList<WindowedValue<T>> elements = new ArrayList<>();

      if (firstRead) {
        try {
          this.reader = unBoundedSource.createReader(null, null);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      boolean available = (firstRead ? reader.start() : reader.advance());

      while (available) {
        elements.add(WindowedValue.timestampedValueInGlobalWindow(reader.getCurrent(), reader.getCurrentTimestamp()));
        available = reader.advance();
      }

      firstRead = false;
      return elements;
    }
  }
}
