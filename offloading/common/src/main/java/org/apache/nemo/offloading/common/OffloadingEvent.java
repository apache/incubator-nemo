package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;

import java.io.Serializable;

public final class OffloadingEvent implements Serializable {

  public enum Type {
    VM_RUN,
    CLIENT_HANDSHAKE,
    WORKER_INIT,
    DATA,
    GBK_START,
    GBK,
    RESULT,
    WARMUP_END,
    END
  }

  private final Type type;
  private final byte[] bytes;
  private final int len;
  private ByteBuf byteBuf;

  public OffloadingEvent(final Type type,
                         final byte[] bytes,
                         final int len) {
    this.type = type;
    this.bytes = bytes;
    this.byteBuf = null;
    this.len = len;
  }

  public OffloadingEvent(final Type type,
                         final ByteBuf byteBuf) {
    this.type = type;
    this.bytes = null;
    this.byteBuf = byteBuf;
    this.len = 0;
  }

  public ByteBuf getByteBuf() {
    return byteBuf;
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
    return "OffloadingEvent:" + type.name();
  }
}
