package org.apache.nemo.runtime.message.netty;

import org.apache.nemo.runtime.message.comm.ControlMessage;

public final class ControlMessageWrapper {

  public enum MsgType {
    Send,
    Request,
    Reply
  }
  public final MsgType type;
  public final ControlMessage.Message msg;

  public ControlMessageWrapper(final MsgType type,
                               final ControlMessage.Message msg) {
    this.type = type;
    this.msg = msg;
  }
}
