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
package edu.snu.nemo.common.ir.vertex;

import edu.snu.nemo.common.ir.Readable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Source vertex with the data in memory.
 * @param <T> type of data.
 */
public final class InMemorySourceVertex<T> extends SourceVertex<T> {
  private Iterable<T> initializedSourceData;

  /**
   * Constructor.
   * @param initializedSourceData the initial data object.
   */
  public InMemorySourceVertex(final Iterable<T> initializedSourceData) {
    this.initializedSourceData = initializedSourceData;
  }

  @Override
  public InMemorySourceVertex<T> getClone() {
    final InMemorySourceVertex<T> that = new InMemorySourceVertex<>(this.initializedSourceData);
    this.copyExecutionPropertiesTo(that);
    return that;
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
  public void clearInternalStates() {
    initializedSourceData = null;
  }

  /**
   * Simply returns the in-memory data.
   * @param <T> type of the data.
   */
  private static final class InMemorySourceReadable<T> implements Readable<T> {
    private final Iterable<T> initializedSourceData;

    /**
     * Constructor.
     * @param initializedSourceData the source data.
     */
    private InMemorySourceReadable(final Iterable<T> initializedSourceData) {
      this.initializedSourceData = initializedSourceData;
    }

    @Override
    public Iterable<T> read() {
      return this.initializedSourceData;
    }

    @Override
    public List<String> getLocations() {
      throw new UnsupportedOperationException();
    }
  }
}
