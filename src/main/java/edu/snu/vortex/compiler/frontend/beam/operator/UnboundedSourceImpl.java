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
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.util.WindowedValue;

import java.util.ArrayList;
import java.util.List;

public final class UnboundedSourceImpl<O> extends Source<O> {
  private final UnboundedSource<O, ?> source;

  public UnboundedSourceImpl(final UnboundedSource<O, ?> source) {
    this.source = source;
  }

  @Override
  public List<Source.Reader<O>> getReaders(final long desiredNumSplits) throws Exception {
    // Can't use lambda due to exception thrown
    final List<Source.Reader<O>> readers = new ArrayList<>();
    for (final UnboundedSource<O, ?> s : source.generateInitialSplits((int)desiredNumSplits, null)) {
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

  public class Reader<T> implements Source.Reader<Element<T>> {
    private final UnboundedSource<T, ?> beamSource;

    Reader(final UnboundedSource<T, ?> beamSource) {
      this.beamSource = beamSource;
    }

    @Override
    public Iterable<Element<T>> read() throws Exception {
      final ArrayList<Element<T>> data = new ArrayList<>();
      try (final UnboundedSource.UnboundedReader<T> reader = beamSource.createReader(null, null)) {
        boolean available = false;
        for (available = reader.start(); available; available = reader.advance()) {
          data.add(new Record<>(WindowedValue.timestampedValueInGlobalWindow(reader.getCurrent(), reader.getCurrentTimestamp())));
        }
      }
      data.add(Watermark.MAX_WATERMARK);
      return data;
    }
  }
}
