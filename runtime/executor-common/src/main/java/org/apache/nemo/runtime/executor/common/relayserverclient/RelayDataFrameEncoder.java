package org.apache.nemo.runtime.executor.common.relayserverclient;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.DataFrameEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public final class RelayDataFrameEncoder extends MessageToMessageEncoder<RelayDataFrame> {
  private static final Logger LOG = LoggerFactory.getLogger(RelayDataFrameEncoder.class.getName());

  private static final int TRANSFER_INDEX_LENGTH = Integer.BYTES;
  private static final int BODY_LENGTH_LENGTH = Integer.BYTES;
  private static final int HEADER_LENGTH = Byte.BYTES + TRANSFER_INDEX_LENGTH + BODY_LENGTH_LENGTH;

  // the maximum length of a frame body. 2**32 - 1
  static final long LENGTH_MAX = 4294967295L;

  private final DataFrameEncoder dataFrameEncoder;

  public RelayDataFrameEncoder() {
    this.dataFrameEncoder = new DataFrameEncoder();
  }

  @Override
  protected void encode(final ChannelHandlerContext ctx, final RelayDataFrame in, final List out) {
    // encode header
    //LOG.info("Encoding relayDataFrame {}", in.dataFrame);

    final String id = in.dstId;
    //final byte[] idBytes = id.getBytes();
    //LOG.info("ID bytes: {}", idBytes);

    final ByteBuf header = ctx.alloc().buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(header);
    try {
      bos.writeChar(0); // 0 means data frame
      bos.writeUTF(id);
      //bos.writeInt(idBytes.length);
      //bos.write(idBytes);
      //bos.writeInt(DataFrameEncoder.HEADER_LENGTH + (int) in.dataFrame.length);
      bos.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    final CompositeByteBuf cbb = ctx.alloc().compositeBuffer(2);

    //final byte[] loggingBytes = new byte[header.readableBytes()];
    //header.getBytes(header.readableBytes(), loggingBytes);
    //LOG.info("Loging bytes in dataFrameEncoder {}", loggingBytes);

    final ByteBuf dataBuf = dataFrameEncoder.encode(ctx, in.dataFrame);

    LOG.info("Relay data {} to {}", id, ctx.channel().remoteAddress());

    cbb.addComponents(true, header, dataBuf);
    out.add(cbb);
  }
}
