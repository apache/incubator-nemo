package org.apache.nemo.runtime.executor.common.relayserverclient;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContextSetupMessage;
import org.apache.nemo.runtime.executor.common.datatransfer.DataFrameEncoder;

import java.io.IOException;
import java.util.List;

public final class RelayControlMessageEncoder extends MessageToMessageEncoder<RelayControlMessage> {


  public RelayControlMessageEncoder() {
  }

  @Override
  protected void encode(final ChannelHandlerContext ctx, final RelayControlMessage in, final List out) {
    // encode header
    final String id = RelayUtils.createId(in.edgeId, in.taskIndex, in.inContext);
    final ByteBuf header = ctx.alloc().buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(header);
    try {
      bos.writeChar(2); // 2 means control message
      bos.writeUTF(id);
      bos.writeInt(in.type.ordinal());
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    // DO sth
    throw new RuntimeException("Not implemented yet");

  }
}