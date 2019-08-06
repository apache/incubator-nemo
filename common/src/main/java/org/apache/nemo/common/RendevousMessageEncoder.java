package org.apache.nemo.common;

import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.nemo.common.RendevousRequest;
import org.apache.nemo.common.RendevousResponse;

import java.util.List;

public final class RendevousMessageEncoder extends MessageToMessageEncoder<Object> {

  public enum Type {
    REQUEST,
    RESPONSE,
    REGISTER
  }

  public RendevousMessageEncoder() {

  }

  @Override
  protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {

    final ByteBufOutputStream bos = new ByteBufOutputStream(ctx.alloc().buffer());

    if (msg instanceof RendevousRequest) {
      bos.writeInt(Type.REQUEST.ordinal());

      final RendevousRequest req = (RendevousRequest) msg;
      bos.writeUTF(req.dst);

    } else if (msg instanceof RendevousResponse) {
      bos.writeInt(Type.RESPONSE.ordinal());

      final RendevousResponse res = (RendevousResponse) msg;
      bos.writeUTF(res.dst);
      bos.writeUTF(res.address);
    } else if (msg instanceof  RendevousRegister) {
      bos.writeInt(Type.REGISTER.ordinal());

      final RendevousRegister req = (RendevousRegister) msg;
      bos.writeUTF(req.dst);
    } else {
      throw new RuntimeException("Unsupported type " + msg);
    }
  }


}
