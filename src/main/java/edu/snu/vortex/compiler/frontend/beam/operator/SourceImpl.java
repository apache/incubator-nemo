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
package edu.snu.vortex.compiler.frontend.beam.operator;

import edu.snu.vortex.compiler.ir.operator.Source;
import org.apache.beam.sdk.io.BoundedSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class SourceImpl<O> extends Source<O> {
  private final BoundedSource<O> source;

  public SourceImpl(final BoundedSource<O> source) {
    this.source = source;
  }

  @Override
  public List<Source.Reader<O>> getReaders(final long desiredBundleSizeBytes) throws Exception {
    return source.splitIntoBundles(desiredBundleSizeBytes, null).stream()
        .map(Reader::new)
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append(", BoundedSource: ");
    sb.append(source);
    return sb.toString();
  }

  public class Reader<T> implements Source.Reader<T> {
    private final BoundedSource<T> beamReader;

    Reader(final BoundedSource<T> beamReader) {
      this.beamReader = beamReader;
    }

    @Override
    public Iterable<T> read() throws Exception {
      final ArrayList<T> data = new ArrayList<>();
      try (final BoundedSource.BoundedReader<T> reader = beamReader.createReader(null)) {
        for (boolean available = reader.start(); available; available = reader.advance()) {
          data.add(reader.getCurrent());
        }
      }
      return data;
    }
  }
}
