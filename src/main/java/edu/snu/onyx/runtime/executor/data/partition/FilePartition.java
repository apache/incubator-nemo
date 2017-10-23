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
package edu.snu.onyx.runtime.executor.data.partition;

import edu.snu.onyx.compiler.ir.Element;
import edu.snu.onyx.runtime.executor.data.FileArea;
import edu.snu.onyx.runtime.executor.data.HashRange;

import java.io.IOException;
import java.util.List;

/**
 * Interface of file partition.
 */
public interface FilePartition {
  /**
   * Writes the serialized data of this partition having a specific hash value as a block to the file
   * where this partition resides.
   *
   * @param serializedData the serialized data which will become a block.
   * @param numElement     the number of elements in the serialized data.
   * @param hashVal        the hash value of this block.
   * @throws IOException if fail to write.
   */
  void writeBlock(final byte[] serializedData,
                         final long numElement,
                         final int hashVal) throws IOException;

  /**
   * Commits the un-committed block metadata.
   */
  void commitRemainderMetadata();

  /**
   * Retrieves the elements of this partition from the file in a specific hash range and deserializes it.
   *
   * @param hashRange the hash range
   * @return an iterable of deserialized elements.
   * @throws IOException if failed to deserialize.
   */
  Iterable<Element> retrieveInHashRange(final HashRange hashRange) throws IOException;

  /**
   * Retrieves the list of {@link FileArea}s for the specified {@link HashRange}.
   *
   * @param hashRange     the hash range
   * @return list of the file areas
   * @throws IOException if failed to open a file channel
   */
  List<FileArea> asFileAreas(final HashRange hashRange) throws IOException;

  /**
   * Deletes the file that contains this partition data.
   * This method have to be called after all read is completed (or failed).
   *
   * @throws IOException if failed to delete.
   */
  void deleteFile() throws IOException;

  /**
   * Commits this partition to prevent further write.
   * If someone "subscribing" the data in this partition, it will be finished.
   *
   * @throws IOException if failed to close.
   */
  void commit() throws IOException;
}
