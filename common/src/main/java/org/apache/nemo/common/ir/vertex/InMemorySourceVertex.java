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

import org.apache.nemo.common.Util;
import org.apache.nemo.common.ir.BoundedIteratorReadable;
import org.apache.nemo.common.ir.Readable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Source vertex with the data in memory.
 *
 * @param <T> type of data.
 */
public final class InMemorySourceVertex<T> extends SourceVertex<T> {
  private Iterable<T> initializedSourceData;

  /**
   * Constructor for InMemorySourceVertex.
   *
   * @param initializedSourceData the initial data object.
   */
  public InMemorySourceVertex(final Iterable<T> initializedSourceData) {
    this.initializedSourceData = initializedSourceData;
  }

  /**
   * Copy Constructor for InMemorySourceVertex.
   *
   * @param that the source object for copying
   */
  private InMemorySourceVertex(final InMemorySourceVertex that) {
    super(that);
    this.initializedSourceData = that.initializedSourceData;
  }

  @Override
  public InMemorySourceVertex<T> getClone() {
    return new InMemorySourceVertex<>(this);
  }

  @Override
  public boolean isBounded() {
    return true;
  }

  @Override
  public List<Readable<T>> getReadables(final int desiredNumOfSplits) throws Exception {

    final List<Readable<T>> readables = new ArrayList<>();
    final long sliceSize = initializedSourceData.spliterator().getExactSizeIfKnown() / desiredNumOfSplits;
    final Iterator<T> iterator = initializedSourceData.iterator();

    for (int i = 0; i < desiredNumOfSplits; i++) {
      final List<T> dataForReader = new ArrayList<>();

      if (i == desiredNumOfSplits - 1) { // final iteration
        iterator.forEachRemaining(dataForReader::add);
      } else {
        for (int j = 0; j < sliceSize && iterator.hasNext(); j++) {
          dataForReader.add(iterator.next());
        }
      }

      readables.add(new InMemorySourceReadable<>(dataForReader));
    }
    return readables;
  }

  @Override
  public String getSourceName() {
    return "InMemorySource";
  }

  @Override
  public long getEstimatedSizeBytes() {
    final ArrayList<Long> list = new ArrayList<>();
    initializedSourceData.spliterator().forEachRemaining(obj -> list.add(Util.getObjectSize(obj)));
    return list.stream().reduce((a, b) -> a + b).orElse(0L);
  }

  @Override
  public void clearInternalStates() {
    initializedSourceData = null;
  }

  /**
   * Simply returns the in-memory data.
   *
   * @param <T> type of the data.
   */
  private static final class InMemorySourceReadable<T> extends BoundedIteratorReadable<T> {

    private final Iterable<T> initializedSourceData;

    /**
     * Constructor.
     *
     * @param initializedSourceData the source data.
     */
    private InMemorySourceReadable(final Iterable<T> initializedSourceData) {
      super();
      this.initializedSourceData = initializedSourceData;
    }

    @Override
    protected Iterator<T> initializeIterator() {
      return initializedSourceData.iterator();
    }

    @Override
    public long readWatermark() {
      throw new UnsupportedOperationException("No watermark");
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
