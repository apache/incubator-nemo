package org.apache.nemo.common;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.io.Serializable;
import java.util.List;

public final class NemoEventCoder {

  public static final class NemoEventEncoder extends MessageToMessageEncoder<NemoEvent> {

    @Override
    protected void encode(ChannelHandlerContext ctx, NemoEvent msg, List<Object> out) throws Exception {
      final ByteBuf buf = ctx.alloc().buffer(4 + msg.getBytes().length);
      buf.writeInt(msg.getType().ordinal());
      buf.writeBytes(msg.getBytes());

      out.add(buf);
    }
  }

  public static final class NemoEventDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
      final int typeOrdinal = msg.readInt();
      // copy the ByteBuf content to a byte array
      byte[] array = new byte[msg.readableBytes()];
      msg.getBytes(0, array);
      out.add(new NemoEvent(NemoEvent.Type.values()[typeOrdinal], array));
    }
  }

}
