package org.apache.nemo.runtime.executor.common.relayserverclient;

import io.netty.buffer.*;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.ControlFrameEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public final class RelayControlFrameEncoder extends MessageToMessageEncoder<RelayControlFrame> {

  private static final Logger LOG = LoggerFactory.getLogger(RelayControlFrameEncoder.class.getName());


  public static final int ZEROS_LENGTH = 5;
  public static final int BODY_LENGTH_LENGTH = Integer.BYTES;
  public static final ByteBuf ZEROS = Unpooled.directBuffer(ZEROS_LENGTH, ZEROS_LENGTH).writeZero(ZEROS_LENGTH);


  public RelayControlFrameEncoder() {
  }

  @Override
  protected void encode(final ChannelHandlerContext ctx, final RelayControlFrame in, final List out) {
    // encode header
    //LOG.info("Encoding relayControlFrame: {}", in.controlMsg);


    final String id = in.dstId;

    final CompositeByteBuf cbb = ctx.alloc().compositeBuffer(4);
    final ByteBuf header = ctx.alloc().buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(header);

    final ByteBuf data = in.controlMsg.encode();
    //final byte[] idBytes = id.getBytes();
    //LOG.info("ID bytes: {}", idBytes);

    try {
      bos.writeChar(1);
      bos.writeUTF(id);
      //bos.writeInt(idBytes.length);
      //bos.write(idBytes);
      //bos.writeInt(ControlFrameEncoder.ZEROS_LENGTH + ControlFrameEncoder.BODY_LENGTH_LENGTH + data.readableBytes());
      bos.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }


    //LOG.info("Encoding relay control frame message {}", in.controlMsg);

    //final byte[] loggingBytes = new byte[header.readableBytes()];
    //header.getBytes(header.readableBytes(), loggingBytes);
    //LOG.info("Loging bytes in controlFrameEncoder {}", loggingBytes);

    cbb.addComponents(true, header, ZEROS.retain(),
      ctx.alloc().ioBuffer(BODY_LENGTH_LENGTH, BODY_LENGTH_LENGTH)
      .writeInt(data.readableBytes()),
      data);

    out.add(cbb);

    /*
    out.add(ZEROS.retain());

    out.add(ctx.alloc().ioBuffer(BODY_LENGTH_LENGTH, BODY_LENGTH_LENGTH)
      .writeInt(data.readableBytes()));

    out.add(data);
    */
  }
}
