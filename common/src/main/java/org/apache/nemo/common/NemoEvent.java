package org.apache.nemo.common;

import java.io.Serializable;

public final class NemoEvent implements Serializable {

  public enum Type {
    CLIENT_HANDSHAKE,
    SIDE,
    MAIN,
    RESULT,
    END
  }

  private final Type type;
  private final byte[] bytes;
  private final int len;

  public NemoEvent(final Type type,
                   final byte[] bytes,
                   final int len) {
    this.type = type;
    this.bytes = bytes;
    this.len = len;
  }

  public Type getType() {
    return type;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public int getLen() {
    return len;
  }

  @Override
  public String toString() {
    return "NemoEvent:" + type.name();
  }
}
