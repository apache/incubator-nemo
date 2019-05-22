package org.apache.nemo.runtime.executor.relayserver;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;

public final class RelayServerDecoder extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(RelayServerDecoder.class);

  private final ConcurrentMap<String, Channel> taskChannelMap;

  private int remainingBytes = 0;
  private String dst;

  public RelayServerDecoder(final ConcurrentMap<String, Channel> taskChannelMap) {
    this.taskChannelMap = taskChannelMap;
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
      } else if (type == 2) {
        final RelayControlMessage.Type controlMsgType = RelayControlMessage.Type.values()[bis.readInt()];
        switch (controlMsgType) {
          case REGISTER: {
            LOG.info("Registering {} / {}", dst, ctx.channel());
            taskChannelMap.put(dst, ctx.channel());
            break;
          }
          case DEREGISTER: {
            LOG.info("Deregistering {} / {}", dst, ctx.channel());
            taskChannelMap.remove(dst);
            break;
          }
        }

      } else {
        throw new RuntimeException("Unsupported type " + type);
      }
    }

    if (remainingBytes > 0) {
      final Channel dstChannel = taskChannelMap.get(dst);

      if (remainingBytes - byteBuf.readableBytes() >= 0) {
        remainingBytes -= byteBuf.readableBytes();
        dstChannel.writeAndFlush(byteBuf);
      } else {
        remainingBytes = 0;
        final ByteBuf bb = byteBuf.retainedSlice(byteBuf.readerIndex(), byteBuf.readerIndex() + remainingBytes);
        dstChannel.writeAndFlush(bb);
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
