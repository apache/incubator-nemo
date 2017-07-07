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

import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.ir.Element;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents a {@link Partition} which is stored in file.
 * It does not contain any actual data.
 */
public final class FilePartition implements Partition {

  private final AtomicBoolean written;
  private Coder coder;
  private String filePath;
  private long numElement;
  private int size;

  /**
   * Constructs a file partition.
   * For the synchronicity of the partition map, it does not write data at the construction time.
   */
  public FilePartition() {
    written = new AtomicBoolean(false);
  }

  /**
   * Writes the serialized data of this partition to a file (synchronously).
   *
   * @param serializedData  the serialized data of this partition.
   * @param coderToSet      the coder used to serialize and deserialize the data of this partition.
   * @param filePathToSet   the path of the file which will contain the data of this partition.
   * @param numElementToSet the number of elements in the serialized data.
   * @throws RuntimeException if failed to write.
   */
  public void writeData(final byte[] serializedData,
                        final Coder coderToSet,
                        final String filePathToSet,
                        final long numElementToSet) throws RuntimeException {
    this.coder = coderToSet;
    this.filePath = filePathToSet;
    this.numElement = numElementToSet;
    this.size = serializedData.length;
    // Wrap the given serialized data (but not copy it)
    final ByteBuffer buf = ByteBuffer.wrap(serializedData);

    // Write synchronously
    try (
        final FileOutputStream fileOutputStream = new FileOutputStream(filePath);
        final FileChannel fileChannel = fileOutputStream.getChannel()
    ) {
      fileChannel.write(buf);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    written.set(true);
  }

  /**
   * Deletes the file that contains this partition data.
   *
   * @throws RuntimeException if failed to delete.
   */
  public void deleteFile() throws RuntimeException {
    if (!written.get()) {
      throw new RuntimeException("This partition is not written yet.");
    }
    try {
      Files.delete(Paths.get(filePath));
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Read the data of this partition from the file and deserialize it.
   *
   * @return an iterable of deserialized data.
   * @throws RuntimeException if failed to deserialize.
   */
  @Override
  public Iterable<Element> asIterable() throws RuntimeException {
    // Read file synchronously
    if (!written.get()) {
      throw new RuntimeException("This partition is not written yet.");
    }

    // Deserialize the data
    // TODO 301: Divide a Task's Output Partitions into Smaller Blocks.
    final ArrayList<Element> deserializedData = new ArrayList<>();
    try (
        final FileInputStream fileStream = new FileInputStream(filePath);
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(fileStream, size)
    ) {
      for (int i = 0; i < numElement; i++) {
        deserializedData.add(coder.decode(bufferedInputStream));
      }
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

    return deserializedData;
  }
}
