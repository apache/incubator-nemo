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
package edu.snu.onyx.compiler.frontend.beam.source;

import edu.snu.onyx.common.ir.vertex.Source;
import org.apache.beam.sdk.io.BoundedSource;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * A wrapper of the Beam boundedSource.
 * @param <T> type of the data read.
 */
public final class BeamBoundedSource<T> implements Source<T> {
  private final BoundedSource<T> boundedSource;

  /**
   * Constructor.
   * @param boundedSource the Beam boundedSource.
   */
  public BeamBoundedSource(final BoundedSource<T> boundedSource) {
    this.boundedSource = boundedSource;
  }

  @Override
  public List<? extends BeamBoundedSource<T>> split(final long var1) throws Exception {
    return boundedSource.split(var1, null).stream()
        .map(BeamBoundedSource<T>::new).collect(Collectors.toList());
  }

  @Override
  public long getEstimatedSizeBytes() throws Exception {
    return boundedSource.getEstimatedSizeBytes(null);
  }

  @Override
  public Source.Reader<T> createReader() throws IOException {
    return new BeamBoundedReader<>(this, boundedSource.createReader(null));
  }

  /**
   * A wrapper of the Beam boundedReader.
   * @param <T> type of the data read.
   */
  class BeamBoundedReader<T> implements Source.Reader<T> {
    private final BeamBoundedSource<T> source;
    private final BoundedSource.BoundedReader<T> boundedReader;

    /**
     * Constructor.
     * @param source source to read.
     * @param boundedReader the Beam boundedReader to wrap.
     */
    BeamBoundedReader(final BeamBoundedSource<T> source, final BoundedSource.BoundedReader<T> boundedReader) {
      this.source = source;
      this.boundedReader = boundedReader;
    }

    @Override
    public boolean start() throws IOException {
      return this.boundedReader.start();
    }

    @Override
    public boolean advance() throws IOException {
      return this.boundedReader.advance();
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      return this.boundedReader.getCurrent();
    }

    @Override
    public void close() throws IOException {
      this.boundedReader.close();
    }

    @Override
    public BeamBoundedSource<T> getCurrentSource() {
      return source;
    }
  }
}
