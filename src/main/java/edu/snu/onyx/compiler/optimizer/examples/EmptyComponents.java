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

import edu.snu.onyx.compiler.ir.Element;
import edu.snu.onyx.compiler.ir.OutputCollector;
import edu.snu.onyx.compiler.ir.Transform;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

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
   */
  public static class EmptyTransform implements Transform {
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
      sb.append("EmptyTransform, name: ");
      sb.append(name);
      return sb.toString();
    }

    @Override
    public void prepare(final Context context, final OutputCollector outputCollector) {
    }

    @Override
    public void onData(final Iterable<Element> data, final String srcVertexId) {
    }

    @Override
    public void close() {
    }
  }

  /**
   * An empty bounded source.
   */
  public static final class EmptyBoundedSource extends BoundedSource {
    private final String name;

    public EmptyBoundedSource(final String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("EmptyBoundedSourceTransform: ");
      sb.append(name);
      return sb.toString();
    }

    public boolean producesSortedKeys(final PipelineOptions options) throws Exception {
      throw new UnsupportedOperationException("Empty bounded source");
    }

    public BoundedReader createReader(final PipelineOptions options) throws IOException {
      throw new UnsupportedOperationException("Empty bounded source");
    }

    @Override
    public List<? extends BoundedSource> split(final long l, final PipelineOptions pipelineOptions) throws Exception {
      return Arrays.asList(this);
    }

    public long getEstimatedSizeBytes(final PipelineOptions options) throws Exception {
      return 1;
    }

    public List<? extends BoundedSource> splitIntoBundles(
        final long desiredBundleSizeBytes, final PipelineOptions options) throws Exception {
      return new ArrayList<>();
    }

    public void validate() {
    }

    public org.apache.beam.sdk.coders.Coder getDefaultOutputCoder() {
      throw new UnsupportedOperationException("Empty bounded source");
    }
  }
}
