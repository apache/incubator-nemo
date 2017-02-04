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
import org.apache.beam.sdk.io.Sink;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

public final class SinkImpl<O> extends edu.snu.vortex.compiler.ir.operator.Sink<O> {
  private final Sink sink;

  public SinkImpl(final Sink sink) {
    this.sink = sink;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append(", BoundedSource: ");
    sb.append(sink);
    return sb.toString();
  }

  @Override
  public Writer getWriter() throws Exception {
    return new Writer<O>(sink);
  }

  public class Writer<T> implements edu.snu.vortex.compiler.ir.operator.Sink.Writer<Element<T>> {
    private final Sink beamSink;

    Writer(final Sink beamSink) {
      this.beamSink = beamSink;
    }

    @Override
    public void write(Iterable<Element<T>> data) throws Exception {
      final Sink.WriteOperation wo = beamSink.createWriteOperation(null);
      wo.initialize(null);
      final Sink.Writer writer = wo.createWriter(null);

      BoundedWindow windowBatch = null;
      for (final Element<T> elem : data) {
        final WindowedValue<KV> wv = (WindowedValue<KV>)elem.asRecord().getWindowedValue();
        final BoundedWindow curWindow = wv.getWindows().iterator().next();

        if (windowBatch == null) {
          // initial: set things up
          windowBatch = curWindow;
          writer.open(windowBatch.toString());
        } else if (curWindow != windowBatch) {
          // new batch: flush and re-set things up
          windowBatch = curWindow;
          writer.close();
          writer.open(curWindow.toString());
        }

        writer.write(wv.getValue());
      }

    }
  }
}
