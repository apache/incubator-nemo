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
package edu.snu.vortex.runtime.executor.data.partition;

import edu.snu.vortex.compiler.ir.Element;

import java.io.IOException;

/**
 * This interface represents a {@link Partition} which is stored in (local or remote) file.
 */
public interface FilePartition extends Partition, AutoCloseable {

  /**
   * Writes the serialized data of this partition as a block to the file where this partition resides.
   *
   * @param serializedData the serialized data of this partition.
   * @param numElement     the number of elements in the serialized data.
   * @throws IOException if fail to write.
   */
  void writeBlock(final byte[] serializedData,
                  final long numElement) throws IOException;

  /**
   * Deletes the file that contains this partition data.
   * This method have to be called after all read is completed (or failed).
   *
   * @throws IOException if failed to delete.
   */
  void deleteFile() throws IOException;

  /**
   * Retrieves the data of this partition from the file in a specific hash range and deserializes it.
   *
   * @param hashRangeStartVal of the hash range (included in the range).
   * @param hashRangeEndVal   of the hash range (excluded from the range).
   * @return an iterable of deserialized data.
   * @throws IOException if failed to deserialize.
   */
  Iterable<Element> retrieveInHashRange(final int hashRangeStartVal,
                                        final int hashRangeEndVal) throws IOException;
}
