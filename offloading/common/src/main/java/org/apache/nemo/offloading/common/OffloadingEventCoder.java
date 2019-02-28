package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

public final class OffloadingEventCoder {

  public static final class OffloadingEventEncoder extends MessageToMessageEncoder<OffloadingEvent> {

    @Override
    protected void encode(ChannelHandlerContext ctx, OffloadingEvent msg, List<Object> out) throws Exception {
      if (msg.getByteBuf() != null) {
        final ByteBuf buf = ctx.alloc().buffer(4);
        buf.writeInt(msg.getType().ordinal());
        System.out.println("Encode " + msg.getType().name() + ", size: " +
          (buf.readableBytes() + msg.getByteBuf().readableBytes()));
        final CompositeByteBuf compositeByteBuf =
          ctx.alloc().compositeBuffer(2).addComponents(true, buf, msg.getByteBuf());
        out.add(compositeByteBuf);
      } else {
        final ByteBuf buf = ctx.alloc().buffer(4 + msg.getLen());
        //System.out.println("Encoded bytes: " + msg.getLen() + 8);
        buf.writeInt(msg.getType().ordinal());
        buf.writeBytes(msg.getBytes(), 0, msg.getLen());

        System.out.println("Encode " + msg.getType().name() + ", size: " + buf.readableBytes());
        out.add(buf);
      }
    }
  }

  public static final class OffloadingEventDecoder extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {

      try {
        final OffloadingEvent.Type type = OffloadingEvent.Type.values()[msg.readInt()];
        System.out.println("Decode message; " + type.name() + ", size: " + msg.readableBytes());
        out.add(new OffloadingEvent(type, msg.retain(1)));
      } catch (final ArrayIndexOutOfBoundsException e) {
        e.printStackTrace();
        throw e;
      }

      /*
      //System.out.println("Decoded bytes: " + msg.readableBytes());
      final int typeOrdinal = msg.readInt();

      // copy the ByteBuf content to a byte array
      //byte[] array = new byte[msg.readableBytes()];
      //msg.readBytes(array);


      try {
        out.add(new OffloadingEvent(OffloadingEvent.Type.values()[typeOrdinal], msg.retain(1)));
      } catch (final ArrayIndexOutOfBoundsException e) {
        e.printStackTrace();
        System.out.println("Type ordinal: " + typeOrdinal);
        throw e;
      }
      */
    }
  }

}
