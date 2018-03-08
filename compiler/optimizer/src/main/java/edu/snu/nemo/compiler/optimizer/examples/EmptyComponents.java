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
package edu.snu.nemo.compiler.optimizer.examples;

import edu.snu.nemo.common.ir.OutputCollector;
import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.vertex.SourceVertex;
import edu.snu.nemo.common.ir.vertex.transform.Transform;

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
    public void onData(final Object element) {
    }

    @Override
    public void close() {
    }
  }

  /**
   * An empty Source Vertex.
   * @param <T> type of the data.
   */
  public static final class EmptySourceVertex<T> extends SourceVertex<T> {
    private final String name;

    /**
     * Constructor.
     * @param name name for the vertex.
     */
    public EmptySourceVertex(final String name) {
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

    @Override
    public List<Readable<T>> getReadables(final int desirednumOfSplits) {
      return Arrays.asList(new EmptyReadable<>());
    }

    @Override
    public EmptySourceVertex<T> getClone() {
      return new EmptySourceVertex<>(this.name);
    }
  }

  /**
   * An empty reader.
   * @param <T> type of the data.
   */
  static final class EmptyReadable<T> implements Readable<T> {
    @Override
    public Iterable<T> read() {
      return new ArrayList<>();
    }
    @Override
    public List<String> getLocations() {
      throw new UnsupportedOperationException();
    }
  }
}
