package org.apache.nemo.runtime.executor.common.relayserverclient;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContextSetupMessage;
import org.apache.nemo.runtime.executor.common.datatransfer.DataFrameEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public final class RelayControlMessageEncoder extends MessageToMessageEncoder<RelayControlMessage> {


  private static final Logger LOG = LoggerFactory.getLogger(RelayControlMessageEncoder.class);

  public RelayControlMessageEncoder() {
  }

  @Override
  protected void encode(final ChannelHandlerContext ctx, final RelayControlMessage in, final List out) {
    // encode header
    final String id = RelayUtils.createId(in.edgeId, in.taskIndex, in.inContext);
    final ByteBuf header = ctx.alloc().buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(header);

    final byte[] idBytes = id.getBytes();
    //LOG.info("ID bytes: {}", idBytes);
    try {
      bos.writeChar(2); // 2 means control message
      bos.writeInt(idBytes.length);
      bos.write(idBytes);
      bos.writeInt(in.type.ordinal());
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    final byte[] loggingBytes = new byte[header.readableBytes()];
    header.getBytes(header.readableBytes(), loggingBytes);

    //LOG.info("Loging bytes in controlMessageEncoder {}", loggingBytes);

    out.add(header);

    LOG.info("Encoding control message {}/{}/{}", in.edgeId, in.taskIndex, in.inContext);
  }
}
