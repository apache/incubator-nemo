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
package edu.snu.nemo.runtime.executor.data;

import java.io.Serializable;

/**
 * A file area descriptor. Used to send file contents without copying or encoding/decoding.
 */
public final class FileArea implements Serializable {
  private final String path;
  private final long position;
  private final long count;

  /**
   * Creates a file area.
   *
   * @param path      the path to the file
   * @param position  the starting position of the area
   * @param count     the length of the area
   */
  public FileArea(final String path, final long position, final long count) {
    this.path = path;
    this.position = position;
    this.count = count;
  }

  /**
   * @return the path to the file
   */
  public String getPath() {
    return path;
  }

  /**
   * @return the starting position of the area
   */
  public long getPosition() {
    return position;
  }

  /**
   * @return the length of the area
   */
  public long getCount() {
    return count;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final FileArea fileArea = (FileArea) o;

    if (position != fileArea.position) {
      return false;
    }
    if (count != fileArea.count) {
      return false;
    }
    return path.equals(fileArea.path);
  }

  @Override
  public int hashCode() {
    int result = path.hashCode();
    result = 31 * result + (int) (position ^ (position >>> 32));
    result = 31 * result + (int) (count ^ (count >>> 32));
    return result;
  }
}
