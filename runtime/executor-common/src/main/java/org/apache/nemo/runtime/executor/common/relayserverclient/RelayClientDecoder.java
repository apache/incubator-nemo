package org.apache.nemo.runtime.executor.common.relayserverclient;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;

public final class RelayClientDecoder extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(RelayClientDecoder.class);


  private int remainingBytes = 0;
  private String dst;

  public RelayClientDecoder() {
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    final ByteBuf byteBuf = (ByteBuf) msg;
    startToRelay(byteBuf, ctx);
  }

  public void startToRelay(final ByteBuf byteBuf, final ChannelHandlerContext ctx) throws Exception {
    if (remainingBytes == 0) {
      final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
      final char type = bis.readChar();
      dst = bis.readUTF();

      if (type == 0 || type == 1) {
        // data frame and control frame
        remainingBytes = bis.readInt();
      } else {
        throw new RuntimeException("Unsupported type " + type);
      }
    }

    if (remainingBytes > 0) {
      if (remainingBytes - byteBuf.readableBytes() >= 0) {
        remainingBytes -= byteBuf.readableBytes();
        ctx.fireChannelRead(byteBuf);
      } else {
        remainingBytes = 0;
        final ByteBuf bb = byteBuf.retainedSlice(byteBuf.readerIndex(), byteBuf.readerIndex() + remainingBytes);
        ctx.fireChannelRead(bb);
        startToRelay(
          byteBuf.retainedSlice(byteBuf.readerIndex() + remainingBytes, byteBuf.readableBytes()), ctx);
      }
    }
  }


  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    LOG.info("Removing channel inactive {}", ctx.channel().remoteAddress().toString());
  }
}
