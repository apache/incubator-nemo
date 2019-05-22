package org.apache.nemo.runtime.executor.common.relayserverclient;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.ControlFrameEncoder;

import java.io.IOException;
import java.util.List;

public final class RelayControlFrameEncoder extends MessageToMessageEncoder<RelayControlFrame> {



  public static final int ZEROS_LENGTH = 5;
  public static final int BODY_LENGTH_LENGTH = Integer.BYTES;
  public static final ByteBuf ZEROS = Unpooled.directBuffer(ZEROS_LENGTH, ZEROS_LENGTH).writeZero(ZEROS_LENGTH);


  public RelayControlFrameEncoder() {
  }

  @Override
  protected void encode(final ChannelHandlerContext ctx, final RelayControlFrame in, final List out) {
    // encode header


    final String id = in.dstId;

    final ByteBuf header = ctx.alloc().buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(header);

    final ByteBuf data = in.controlMsg.encode();

    try {
      bos.writeChar(1);
      bos.writeUTF(id);
      bos.writeInt(ControlFrameEncoder.ZEROS_LENGTH + ControlFrameEncoder.BODY_LENGTH_LENGTH + data.readableBytes());
      bos.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }


    out.add(header);

    out.add(ZEROS.retain());
    //out.add(ctx.alloc().ioBuffer(BODY_LENGTH_LENGTH, BODY_LENGTH_LENGTH)
    //  .writeInt(frameBody.length));

    out.add(ctx.alloc().ioBuffer(BODY_LENGTH_LENGTH, BODY_LENGTH_LENGTH)
      .writeInt(data.readableBytes()));

    out.add(data);
  }
}
