package org.apache.nemo.offloading.common;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

public final class OffloadingEventCoder {

  public static final class OffloadingEventEncoder extends MessageToMessageEncoder<OffloadingEvent> {

    @Override
    protected void encode(ChannelHandlerContext ctx, OffloadingEvent msg, List<Object> out) throws Exception {
      if (msg.getByteBuf() != null) {
        final ByteBuf buf = ctx.alloc().buffer(8);
        buf.writeInt(msg.getType().ordinal());
        buf.writeBoolean(false); // no data
        out.add(buf);
        out.add(msg.getByteBuf());
      } else {
        final ByteBuf buf = ctx.alloc().buffer(8 + msg.getLen());
        //System.out.println("Encoded bytes: " + msg.getLen() + 8);
        buf.writeInt(msg.getType().ordinal());
        buf.writeBoolean(true); // no data
        buf.writeBytes(msg.getBytes(), 0, msg.getLen());
        out.add(buf);
      }
    }
  }

  public static final class OffloadingEventDecoder extends MessageToMessageDecoder<ByteBuf> {
    private boolean isControlMessage = true;
    private OffloadingEvent.Type type;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {

      if (isControlMessage) {
        type = OffloadingEvent.Type.values()[msg.readInt()];
        final boolean hasDataInThisBuffer = msg.readBoolean();
        System.out.println("Decode control message; " + type.name() +" hasData: " + hasDataInThisBuffer);
        if (hasDataInThisBuffer) {
          System.out.println("Data for " + type.name());
          out.add(new OffloadingEvent(type, msg.retain(1)));
        } else {
          if (msg.readableBytes() > 0) {
            throw new RuntimeException("Readbale byte is larger than 0 for control msg: " + type.name() + ", " + msg.readableBytes());
          }
          isControlMessage = false;
          msg.release();
        }
      } else {
        System.out.println("Data message of " + type.name());
        isControlMessage = true;
        try {
          out.add(new OffloadingEvent(type, msg.retain(1)));
        } catch (final ArrayIndexOutOfBoundsException e) {
          e.printStackTrace();
          System.out.println("Type ordinal: " + type.name());
          throw e;
        }
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
