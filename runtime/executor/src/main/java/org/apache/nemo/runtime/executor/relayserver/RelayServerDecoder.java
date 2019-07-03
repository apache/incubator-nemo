package org.apache.nemo.runtime.executor.relayserver;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public final class RelayServerDecoder extends ByteToMessageDecoder {
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
  private boolean waitingStr = false;

  private final Map<String, List<ByteBuf>> pendingBytes = new HashMap<>();

  public RelayServerDecoder(final ConcurrentMap<String, Channel> taskChannelMap) {
    this.taskChannelMap = taskChannelMap;
  }

  private void startToRelay(final ByteBuf byteBuf, final ChannelHandlerContext ctx) throws Exception {

    while (byteBuf.readableBytes() > 0) {
      //LOG.info("Remaining bytes: {} readable: {}", remainingBytes, byteBuf.readableBytes());

      switch (status) {
        case WAITING_HEADER1: {
          if (byteBuf.readableBytes() < 6) {
            //LOG.info("Waiting for 6 more bytes... {}", byteBuf.readableBytes());
            return;
          } else {
            type = byteBuf.readChar();
            idLength = byteBuf.readInt();
            status = Status.WAITING_HEADER2;
          }
        }
        case WAITING_HEADER2: {
          if (byteBuf.readableBytes() < idLength + 4) {
            //LOG.info("Waiting for {} bytes... {}", idLength + 4, byteBuf.readableBytes());
            waitingStr = true;
            return;
          } else {
            final byte[] idBytes = new byte[idLength];
            byteBuf.readBytes(idBytes);
            //LOG.info("ID bytes: {}", idBytes);

            /*
            if (waitingStr) {
              final int readableBytes = byteBuf.readableBytes();
              final byte[] logginBytes = new byte[readableBytes];
              byteBuf.getBytes(readableBytes, logginBytes);
              LOG.info("logging bytes: {}", logginBytes);
            }
            */

            dst = new String(idBytes);

            //LOG.info("Dst: {}, readable: {}", dst, byteBuf.readableBytes());

            if (type == 0 || type == 1) {
              // data frame and control frame
              remainingBytes = byteBuf.readInt();
              status = Status.WAITING_DATA;
            } else if (type == 2) {
              // control message
              status = Status.WAITING_HEADER1;

              final RelayControlMessage.Type controlMsgType = RelayControlMessage.Type.values()[byteBuf.readInt()];
              switch (controlMsgType) {
                case REGISTER: {
                  LOG.info("Registering {} / {}", dst, ctx.channel());
                  taskChannelMap.putIfAbsent(dst, ctx.channel());
                  synchronized (pendingBytes) {
                    if (pendingBytes.get(dst) != null) {
                      LOG.info("Flushing pending byte {} / {}", dst, ctx.channel());
                      for (final ByteBuf pendingByte : pendingBytes.get(dst)) {
                        ctx.channel().write(pendingByte);
                      }
                      ctx.channel().flush();
                      pendingBytes.remove(dst);
                    }
                  }
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
            final int maxRead = Math.min(remainingBytes, byteBuf.readableBytes());
            final ByteBuf bb = byteBuf.readRetainedSlice(maxRead);

            if (!taskChannelMap.containsKey(dst)) {
              LOG.info("Pending for dst {}... readable: {}", dst, byteBuf.readableBytes());
              synchronized (pendingBytes) {
                pendingBytes.putIfAbsent(dst, new ArrayList<>());
                pendingBytes.get(dst).add(bb);
              }
            } else {
              final Channel dstChannel = taskChannelMap.get(dst);
              LOG.info("Sending data to {}", dst);

              dstChannel.writeAndFlush(bb);

              //LOG.info("Forward data to dst {}... read: {}, readable: {}, remaining: {}", dst, maxRead,
              //  byteBuf.readableBytes(), remainingBytes);
            }

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

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    startToRelay(in, ctx);
  }
}
