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
package edu.snu.onyx.common.ir.vertex;

import edu.snu.onyx.common.ir.Reader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Source vertex with initial data as object.
 * @param <T> type of initial data.
 */
public final class InitializedSourceVertex<T> extends SourceVertex<T> {
  private final Iterable<T> initializedSourceData;

  /**
   * Constructor.
   * @param initializedSourceData the initial data object.
   */
  public InitializedSourceVertex(final Iterable<T> initializedSourceData) {
    this.initializedSourceData = initializedSourceData;
  }

  @Override
  public InitializedSourceVertex<T> getClone() {
    final InitializedSourceVertex<T> that = new InitializedSourceVertex<>(this.initializedSourceData);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public List<Reader<T>> getReaders(final int desiredNumOfSplits) throws Exception {
    final List<Reader<T>> readers = new ArrayList<>();
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

      readers.add(new InitializedSourceReader<>(dataForReader));
    }
    return readers;
  }

  /**
   * Reader for initialized source vertex. It simply returns the initialized data.
   * @param <T> type of the initial data.
   */
  public class InitializedSourceReader<T> implements Reader<T> {
    private final Iterable<T> initializedSourceData;

    /**
     * Constructor.
     * @param initializedSourceData the source data.
     */
    InitializedSourceReader(final Iterable<T> initializedSourceData) {
      this.initializedSourceData = initializedSourceData;
    }

    @Override
    public final Iterator<T> read() throws Exception {
      return this.initializedSourceData.iterator();
    }
  }
}
