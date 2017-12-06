package edu.snu.onyx.common;

import java.io.ByteArrayOutputStream;

/**
 * This class represents a custom implementation of {@link ByteArrayOutputStream},
 * which enables to get bytes buffer directly (without memory copy).
 */
public final class DirectByteArrayOutputStream extends ByteArrayOutputStream {

  public DirectByteArrayOutputStream() {
    super();
  }

  public DirectByteArrayOutputStream(final int size) {
    super(size);
  }

  public byte[] getBufDirectly() {
    return buf;
  }

  public int getCount() {
    return count;
  }
}
