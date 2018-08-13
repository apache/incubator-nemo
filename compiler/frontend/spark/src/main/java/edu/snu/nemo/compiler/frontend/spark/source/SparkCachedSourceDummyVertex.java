/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.compiler.frontend.spark.source;

import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.vertex.SourceVertex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Bounded source vertex for cached data.
 * It does not have actual data but just wraps the cached input data.
 */
public final class SparkCachedSourceDummyVertex<T> extends SourceVertex<T> {
  private List<Readable<T>> readables;

  /**
   * Constructor.
   *
   * @param numPartitions the number of partitions.
   */
  public SparkCachedSourceDummyVertex(final int numPartitions) {
    this.readables = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      readables.add(new CachedReadable());
    }
  }

  /**
   * Constructor for cloning.
   *
   * @param readables the list of Readables to set.
   */
  private SparkCachedSourceDummyVertex(final List<Readable<T>> readables) {
    this.readables = readables;
  }

  @Override
  public SparkCachedSourceDummyVertex getClone() {
    final SparkCachedSourceDummyVertex that = new SparkCachedSourceDummyVertex<>(this.readables);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public List<Readable<T>> getReadables(final int desiredNumOfSplits) {
    // Ignore the desired number of splits.
    return readables;
  }

  @Override
  public void clearInternalStates() {
    readables = null;
  }

  /**
   * A Readable wrapper for cached data.
   * It does not contain any actual data but the data will be sent from the cached store through external input reader.
   */
  private final class CachedReadable implements Readable<T> {

    /**
     * Constructor.
     */
    private CachedReadable() {
      // Do nothing
    }

    @Override
    public Iterable<T> read() throws IOException {
      return Collections.emptyList();
    }

    @Override
    public List<String> getLocations() {
      throw new UnsupportedOperationException();
    }
  }
}
