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
package edu.snu.vortex.runtime.common;

import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests to verify the correctness of {@link LocalFileManager}'s behaviors.
 */
public class LocalFileManagerTest {
  private static final String ROOT_FILE_SPACE = "./";
  private static final String FILE_SPACE_NAME_PREFIX = "file-space-";
  private static final int NUM_FILE_SPACE = 10;
  private static final int NUM_SUB_DIRS_PER_FSPACE = 64;
  private LocalFileManager localFileManager;

  @Before
  public void setup() throws IOException {
    this.localFileManager = new LocalFileManager(createFileSpaces(NUM_FILE_SPACE), NUM_SUB_DIRS_PER_FSPACE);
  }

  @Test
  public void testAllocateMultipleFiles() {
    final int numFiles = 1024;
    final String fileNamePrefix = "file-";
    final byte[] writeBuffer = new byte[256];
    final byte[] readBuffer = new byte[256];
    List<File> files = new ArrayList<>();

    generateByteSequence(writeBuffer, writeBuffer.length);

    try {
      for (int i = 0; i < numFiles; i++) {
        final File file = localFileManager.getFileByName(fileNamePrefix + i);
        assertEquals(fileNamePrefix + i, file.getName());
        file.deleteOnExit();
        files.add(file);
      }

      Iterator<File> iterator = files.iterator();
      while(iterator.hasNext()) {
        final File file = iterator.next();
        final FileOutputStream out = new FileOutputStream(file);
        final FileInputStream in = new FileInputStream(file);

        out.write(writeBuffer);
        out.flush();

        in.read(readBuffer);
        assertTrue(areEqual(writeBuffer, readBuffer, writeBuffer.length));

        out.close();
        in.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("An IOException occurs during file I/O");
    }
  }

  @Test
  public void testReadExistingFiles() {
    final String fileName = "tempfile";
    final byte[] writeBuffer = new byte[256];
    final byte[] readBuffer = new byte[256];

    generateByteSequence(writeBuffer, writeBuffer.length);

    try {
      final File file1 = localFileManager.getFileByName(fileName);
      final FileOutputStream out = new FileOutputStream(file1);
      file1.deleteOnExit();
      out.write(writeBuffer);
      out.close();

      final File file2 = localFileManager.getFileByName(fileName);
      final FileInputStream in = new FileInputStream(file2);
      file2.deleteOnExit();
      in.read(readBuffer);
      in.close();

      assertTrue(areEqual(writeBuffer, readBuffer, writeBuffer.length));
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("An IOException occurs during file I/O");
    }
  }

  private List<File> createFileSpaces(final int numSubFileSpaces) {
    final List<File> fileSpaces = new ArrayList<>(numSubFileSpaces);

    for (int i = 0; i < numSubFileSpaces; i++) {
      final File fileSpace = new File(ROOT_FILE_SPACE + FILE_SPACE_NAME_PREFIX + i);
      fileSpace.mkdir();
      fileSpace.deleteOnExit();
      fileSpaces.add(fileSpace);
    }

    return fileSpaces;
  }

  private boolean areEqual(final byte [] byteArray1, final byte [] byteArray2, final int length) {
    for (int i = 0; i < length; i++) {
      if (byteArray1[i] != byteArray2[i]) {
        return false;
      }
    }

    return true;
  }

  private void generateByteSequence(final byte [] byteArray, final int length) {
    for (int i = 0; i < length; i++) {
      byteArray[i] = (byte) (i % 0xff);
    }
  }
}
