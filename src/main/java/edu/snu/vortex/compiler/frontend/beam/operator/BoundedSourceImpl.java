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

import edu.snu.vortex.compiler.frontend.beam.element.Element;
import edu.snu.vortex.compiler.frontend.beam.element.Record;
import edu.snu.vortex.compiler.frontend.beam.element.Watermark;
import edu.snu.vortex.compiler.ir.operator.Source;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.util.WindowedValue;

import java.util.ArrayList;
import java.util.List;

public final class BoundedSourceImpl<O> extends Source<O> {
  private final BoundedSource<O> source;

  public BoundedSourceImpl(final BoundedSource<O> source) {
    this.source = source;
  }

  @Override
  public List<Source.Reader<O>> getReaders(final long desiredBundleSizeBytes) throws Exception {
    final List<Source.Reader<O>> readers = new ArrayList<>();
    for (final BoundedSource<O> s : source.splitIntoBundles(desiredBundleSizeBytes, null)) {
      readers.add(new Reader(s));
    }
    return readers;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append(", BoundedSource: ");
    sb.append(source);
    return sb.toString();
  }

  public class Reader<T extends BoundedSource> implements Source.Reader<Element<T>> {
    private final BoundedSource<T> beamReader;

    Reader(final BoundedSource<T> beamReader) {
      this.beamReader = beamReader;
    }

    @Override
    public Iterable<Element<T>> read() throws Exception {
      final ArrayList<Element<T>> data = new ArrayList<>();
      try (final BoundedSource.BoundedReader<T> reader = beamReader.createReader(null)) {
        for (boolean available = reader.start(); available; available = reader.advance()) {
          data.add(new Record<>(WindowedValue.valueInGlobalWindow(reader.getCurrent())));
        }
      }
      data.add(Watermark.MAX_WATERMARK);
      return data;
    }
  }
}
