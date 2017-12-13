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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Interface to read bounded data.
 * It is to be implemented in the compiler frontend, possibly for every operator in a dataflow language.
 * @param <T> type of the data read.
 */
public interface Source<T> extends Serializable {
  /**
   * Splits the source into bundles of desiredBundleSizeBytes, approximately.
   */
  List<? extends Source<T>> split(long var1) throws Exception;

  /**
   * An estimate of the total size (in bytes) of the data that would be read from this source.
   * This estimate is in terms of external storage size, before any decompression or other
   * processing done by the reader.
   */
  long getEstimatedSizeBytes() throws Exception;

  /**
   * @return a new Reader that reads from this source.
   * @throws IOException exception while reading.
   */
  Source.Reader<T> createReader() throws IOException;

  /**
   * The interface for custom input sources readers.
   * @param <T> type of the data read.
   */
  interface Reader<T> extends AutoCloseable {
    /**
     * Initializes the reader and advances the reader to the first record.
     * @return whether or not it has succeeded.
     * @throws IOException exception while reading.
     */
    boolean start() throws IOException;

    /**
     * Advances the reader to the next valid record.
     * @return whether or not it has succeeded.
     * @throws IOException exception while reading.
     */
    boolean advance() throws IOException;

    /**
     * Closes the reader. The reader cannot be used after this method is called.
     * @throws IOException exception while reading.
     */
    void close() throws IOException;

    /**
     * @return the value of the data item that was read by the last start or advance call. The returned value
     * should be effectively immutable and remain valid indefinitely.
     * @throws NoSuchElementException exception if there are no such element.
     */
    T getCurrent() throws NoSuchElementException;

    /**
     * @return a source describing the same input that this Reader reads.
     */
    Source<T> getCurrentSource();
  }
}
