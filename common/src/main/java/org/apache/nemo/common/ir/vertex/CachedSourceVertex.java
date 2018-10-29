/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.common.ir.vertex;

import org.apache.nemo.common.ir.Readable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Bounded source vertex for cached data.
 * It does not have actual data but just wraps the cached input data.
 * @param <T> the type of data to emit.
 */
public final class CachedSourceVertex<T> extends SourceVertex<T> {
  private List<Readable<T>> readables;

  /**
   * Constructor.
   *
   * @param numPartitions the number of partitions.
   */
  public CachedSourceVertex(final int numPartitions) {
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
  private CachedSourceVertex(final List<Readable<T>> readables) {
    this.readables = readables;
  }

  @Override
  public CachedSourceVertex getClone() {
    final CachedSourceVertex that = new CachedSourceVertex<>(this.readables);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public boolean isBounded() {
    // It supports only bounded source.
    return true;
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
    public void prepare() {

    }

    @Override
    public T readCurrent() {
      throw new UnsupportedOperationException(
        "CachedSourceVertex should not be used");
    }

    @Override
    public void advance() throws IOException {
      throw new UnsupportedOperationException(
        "CachedSourceVertex should not be used");
    }

    @Override
    public long readWatermark() {
      throw new UnsupportedOperationException(
        "CachedSourceVertex should not be used");
    }

    @Override
    public boolean isFinished() {
      return true;
    }

    @Override
    public List<String> getLocations() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {

    }
  }
}
