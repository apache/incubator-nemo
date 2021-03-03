package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;

import java.io.Serializable;

public final class OffloadingMasterEvent implements Serializable {

  public enum Type {
    SEND_ADDRESS,
    CLIENT_HANDSHAKE,
    CONNECT,
    CONNECT_DONE,
    WORKER_INIT,
    WORKER_INIT_DONE,
    ACTIVATE,
    TASK_READY,
    DATA,
    TASK_SEND, // **
    TASK_FINISH, // **
    GBK_START,
    GBK,
    RESULT,
    WARMUP_END,
    END,
    DUPLICATE_REQUEST_TERMIATION,
    CPU_LOAD,

    /// for vm scaling info
    VM_SCALING_INFO,


    // master -> worker
    EXECUTOR_INIT_INFO,
    EXECUTOR_FINISH_INFO, // send task-executor id map
    // worker -> vm worker
    OFFLOADING_TASK,
    MIDDLE_TASK,
    SOURCE_TASK,
    TASK_FINISH_EVENT,
  }

  private final Type type;
  private final byte[] bytes;
  private final int len;
  private ByteBuf byteBuf;

  public OffloadingMasterEvent(final Type type,
                               final byte[] bytes,
                               final int len) {
    this.type = type;
    this.bytes = bytes;
    this.byteBuf = null;
    this.len = len;
  }

  public OffloadingMasterEvent(final Type type,
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
    return "OffloadingMasterEvent:" + type.name();
  }
}
