package org.apache.nemo.runtime.executor.data;

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
