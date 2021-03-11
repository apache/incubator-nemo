package org.apache.nemo.runtime.message.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.nemo.runtime.common.comm.ControlMessage;

import java.util.List;

public final class NettyControlMessageDecoder extends MessageToMessageDecoder<ByteBuf> {



  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
    final ByteBufInputStream bis = new ByteBufInputStream(msg);
    final ControlMessage.Message message = ControlMessage.Message.parseFrom(bis);
    out.add(message);
  }
}
