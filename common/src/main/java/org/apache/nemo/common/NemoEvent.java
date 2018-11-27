package org.apache.nemo.common;

import java.io.Serializable;

public final class NemoEvent implements Serializable {

  public enum Type {
    CLIENT_HANDSHAKE,
    SIDE,
    MAIN,
    END
  }

  private final Type type;
  private final byte[] bytes;

  public NemoEvent(final Type type,
                   final byte[] bytes) {
    this.type = type;
    this.bytes = bytes;
  }

  public Type getType() {
    return type;
  }

  public byte[] getBytes() {
    return bytes;
  }

  @Override
  public String toString() {
    return "NemoEvent:" + type.name();
  }
}
