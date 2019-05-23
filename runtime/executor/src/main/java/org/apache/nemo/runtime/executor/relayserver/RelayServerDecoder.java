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

  public enum Status {
    WAITING_HEADER1,
    WAITING_HEADER2,
    WAITING_DATA
  }

  private Status status = Status.WAITING_HEADER1;

  private char type;
  private int idLength;
  private ByteBufInputStream bis;

  public RelayServerDecoder(final ConcurrentMap<String, Channel> taskChannelMap) {
    this.taskChannelMap = taskChannelMap;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    final ByteBuf byteBuf = (ByteBuf) msg;
    startToRelay(byteBuf, ctx);
  }

  public void startToRelay(final ByteBuf byteBuf, final ChannelHandlerContext ctx) throws Exception {


    while (byteBuf.readableBytes() > 0) {
      LOG.info("Remaining bytes: {} readable: {}", remainingBytes, byteBuf.readableBytes());

      switch (status) {
        case WAITING_HEADER1: {
          if (byteBuf.readableBytes() < 5) {
            LOG.info("Waiting for 5 more bytes... {}", byteBuf.readableBytes());
            return;
          } else {
            bis = new ByteBufInputStream(byteBuf);
            type = bis.readChar();
            idLength = bis.readInt();
            status = Status.WAITING_HEADER2;
          }
        }
        case WAITING_HEADER2: {
          if (byteBuf.readableBytes() < idLength + 4) {
            LOG.info("Waiting for {} bytes... {}", idLength + 4, byteBuf.readableBytes());
            return;
          } else {
            final byte[] idBytes = new byte[idLength];
            bis.read(idBytes);
            dst = new String(idBytes);

            LOG.info("Dst: {}", dst);

            if (type == 0 || type == 1) {
              // data frame and control frame
              remainingBytes = bis.readInt();
              status = Status.WAITING_DATA;
            } else if (type == 2) {
              // control message
              status = Status.WAITING_HEADER1;

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

              break;
            } else {
              throw new RuntimeException("Unsupported type " + type);
            }
          }
        }
        case WAITING_DATA: {
          if (remainingBytes > 0) {
            final Channel dstChannel = taskChannelMap.get(dst);
            final int maxRead = Math.min(remainingBytes, byteBuf.readableBytes());
            final ByteBuf bb = byteBuf.readRetainedSlice(maxRead);
            dstChannel.writeAndFlush(bb);

            LOG.info("Forward data to dst {}... read: {}, readable: {}, remaining: {}", dst, maxRead,
              byteBuf.readableBytes(), remainingBytes);

            remainingBytes -= maxRead;

            if (remainingBytes == 0) {
              status = Status.WAITING_HEADER1;
            }
          }
          break;
        }
      }
    }
  }


  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    LOG.info("Removing channel inactive {}", ctx.channel().remoteAddress().toString());
  }
}
