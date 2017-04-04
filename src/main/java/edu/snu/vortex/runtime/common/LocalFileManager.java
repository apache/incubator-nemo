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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A simple local file manager that manages file spaces and directory structures inside them to allocate local files.
 */
public final class LocalFileManager {
  private static final String ROOT_DIR_NAME = "LocalFileManager";
  private final List<File> rootDirs;
  private final List<List<File>> subDirs;
  private final int numSubDirsPerRootDir;

  public LocalFileManager(final List<File> fileSpaces, final int numSubDirsPerRootDir) throws IOException {
    final int numFileSpaces = fileSpaces.size();
    this.rootDirs = new ArrayList<>(numFileSpaces);
    this.subDirs = new ArrayList<>(numFileSpaces);
    this.numSubDirsPerRootDir = numSubDirsPerRootDir;

    fileSpaces.forEach(fs -> {
      final File rootDir = new File(fs, ROOT_DIR_NAME);

      rootDir.mkdir();
      rootDir.deleteOnExit();
      this.rootDirs.add(rootDir);
    });

    for (int i = 0; i < numFileSpaces; i++) {
      final List<File> subDirsPerRoot = new ArrayList<>(numSubDirsPerRootDir);

      for (int j = 0; j < numSubDirsPerRootDir; j++) {
        final File newDir = new File(rootDirs.get(i), String.format("%02x", j));
        if (!newDir.exists() && !newDir.mkdir()) {
          throw new IOException("failed to create a directory in " + newDir.getParent());
        }

        newDir.deleteOnExit();
        subDirsPerRoot.add(newDir);
      }
      this.subDirs.add(subDirsPerRoot);
    }
  }

  /**
   * Finds a file with a given file name.
   * Currently it doesn't support users to include directory path in the file name.
   * @param fileName name of the file to find.
   * @return a file object associated to the requested file.
   * @throws IOException thrown when it fails to find a subdirectory where the target file should be located.
   * Additionally, it is thrown if any io failures occur during file creation.
   */
  public File getFileByName(final String fileName) throws IOException {
    final int hashCode = Math.abs(fileName.hashCode());
    final int rootDirIdx = hashCode % rootDirs.size();
    final int subDirIdx = (hashCode / rootDirs.size()) % numSubDirsPerRootDir;
    File file;

    synchronized (subDirs.get(rootDirIdx)) {
      final File subDir = subDirs.get(rootDirIdx).get(subDirIdx);
      if (subDir == null) {
        throw new FileNotFoundException("failed to find the sub directory " + subDir.getPath());
      }

      file = new File(subDir, fileName);
      file.createNewFile(); //The named file will be created if it doesn't exist.
    }

    return file;
  }

}
