package edu.snu.coral.common;

import java.io.ByteArrayOutputStream;

/**
 * This class represents a custom implementation of {@link ByteArrayOutputStream},
 * which enables to get bytes buffer directly (without memory copy).
 */
public final class DirectByteArrayOutputStream extends ByteArrayOutputStream {

  /**
   * Default constructor.
   */
  public DirectByteArrayOutputStream() {
    super();
  }

  /**
   * Constructor specifying the size.
   * @param size the initial size.
   */
  public DirectByteArrayOutputStream(final int size) {
    super(size);
  }

  /**
   * @return the buffer where data is stored.
   */
  public byte[] getBufDirectly() {
    return buf;
  }

  /**
   * @return the number of valid bytes in the buffer.
   */
  public int getCount() {
    return count;
  }
}
