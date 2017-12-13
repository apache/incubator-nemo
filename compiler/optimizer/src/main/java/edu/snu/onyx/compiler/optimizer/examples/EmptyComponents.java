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
package edu.snu.onyx.compiler.optimizer.examples;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.common.ir.OutputCollector;
import edu.snu.onyx.common.ir.vertex.Source;
import edu.snu.onyx.common.ir.vertex.transform.Transform;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Empty components to mock transform and source, for tests and examples.
 */
public class EmptyComponents {

  /**
   * An empty transform.
   * @param <I> input type.
   * @param <O> output type.
   */
  public static class EmptyTransform<I, O> implements Transform<I, O> {
    private final String name;

    /**
     * Default constructor.
     * @param name name of the empty transform.
     */
    public EmptyTransform(final String name) {
      this.name = name;
    }

    @Override
    public final String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append(super.toString());
      sb.append(", name: ");
      sb.append(name);
      return sb.toString();
    }

    @Override
    public void prepare(final Context context, final OutputCollector<O> outputCollector) {
    }

    @Override
    public void onData(final Iterable<I> elements, final String srcVertexId) {
    }

    @Override
    public void close() {
    }
  }

  /**
   * An empty bounded source.
   */
  public static final class EmptyBoundedSource implements Source {
    private final String name;

    public EmptyBoundedSource(final String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append(super.toString());
      sb.append(", name: ");
      sb.append(name);
      return sb.toString();
    }

    public boolean producesSortedKeys() throws Exception {
      throw new UnsupportedOperationException("Empty bounded source");
    }

    public Reader createReader() throws IOException {
      throw new UnsupportedOperationException("Empty bounded source");
    }

    @Override
    public List<? extends Source> split(final long l) throws Exception {
      return Arrays.asList(this);
    }

    public long getEstimatedSizeBytes() throws Exception {
      return 0;
    }

    public List<? extends Source> splitIntoBundles(final long desiredBundleSizeBytes) throws Exception {
      return new ArrayList<>();
    }

    public void validate() {
    }

    public Coder getDefaultOutputCoder() {
      throw new UnsupportedOperationException("Empty bounded source");
    }
  }
}
