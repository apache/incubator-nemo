package org.apache.nemo.runtime.executor.relayserver;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.nemo.runtime.executor.common.OutputWriterFlusher;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class RelayServerDecoder extends ByteToMessageDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(RelayServerDecoder.class);

  private final ConcurrentMap<String, Channel> taskChannelMap;
  private final ConcurrentMap<String, List<ByteBuf>> pendingByteMap;

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

  private final ScheduledExecutorService pendingFlusher;
  private final OutputWriterFlusher outputWriterFlusher;

  public RelayServerDecoder(final ConcurrentMap<String, Channel> taskChannelMap,
                            final ConcurrentMap<String, List<ByteBuf>> pendingByteMap,
                            final OutputWriterFlusher outputWriterFlusher) {
    this.taskChannelMap = taskChannelMap;
    this.pendingByteMap = pendingByteMap;
    this.pendingFlusher = Executors.newSingleThreadScheduledExecutor();
    this.outputWriterFlusher = outputWriterFlusher;

    pendingFlusher.scheduleAtFixedRate(() -> {
      try {

        for (final String dstKey : taskChannelMap.keySet()) {
          if (pendingByteMap.containsKey(dstKey)) {
            final List<ByteBuf> pendingBytes = pendingByteMap.remove(dstKey);
            final Channel channel = taskChannelMap.get(dstKey);

            if (pendingBytes != null) {
              synchronized (pendingBytes) {
                LOG.info("Flushing pending byte {} size: {} / {}", dstKey, pendingBytes.size(), channel);
                for (final ByteBuf pendingByte : pendingBytes) {
                  channel.write(pendingByte);
                }
                channel.flush();
                pendingBytes.clear();
              }
            }
          }
        }

      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

    }, 1, 1, TimeUnit.SECONDS);
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
          break;
        }
        case WAITING_HEADER2: {
          if (byteBuf.readableBytes() < idLength + 4) {
            //LOG.info("Waiting for {} bytes... {}", idLength + 4, byteBuf.readableBytes());
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

                  if (taskChannelMap.containsKey(dst)) {
                    LOG.info("Duplicate dst channel in relayServer.. just overwrite {}", dst);
                  }

                  taskChannelMap.put(dst, ctx.channel());

                  outputWriterFlusher.registerChannel(ctx.channel());

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
          break;
        }
        case WAITING_DATA: {
          if (byteBuf.readableBytes() >= remainingBytes) {

            final int maxRead = Math.min(remainingBytes, byteBuf.readableBytes());
            final ByteBuf bb = byteBuf.readRetainedSlice(maxRead);

            if (!taskChannelMap.containsKey(dst)) {
              LOG.info("Pending for dst {}... readable: {}", dst, bb.readableBytes());
              pendingByteMap.putIfAbsent(dst, new ArrayList<>());
              final List<ByteBuf> pendingBytes = pendingByteMap.get(dst);

              if (pendingBytes != null) {

                synchronized (pendingBytes) {
                  pendingBytes.add(bb);
                  LOG.info("Add {} byte to pendingBytes... size: {}", pendingBytes.size());
                  pendingByteMap.put(dst, pendingBytes);
                }
              } else {

                final Channel dstChannel = taskChannelMap.get(dst);

                /*
                if (type == 1) {
                  LOG.info("Sending data to {}, readBytes: {}, channel: {}, active: {}, open: {}", dst, remainingBytes,
                    dstChannel, dstChannel.isActive(), dstChannel.isOpen());
                }
                */

                dstChannel.writeAndFlush(bb);
              }
            } else {

              final Channel dstChannel = taskChannelMap.get(dst);

              if (pendingByteMap.containsKey(dst)) {
                final List<ByteBuf> pendingBytes = pendingByteMap.remove(dst);

                if (pendingBytes != null) {
                  synchronized (pendingBytes) {
                    LOG.info("Flushing pending byte {} size: {} / {}", dst, pendingBytes.size(), dstChannel);
                    for (final ByteBuf pendingByte : pendingBytes) {
                      dstChannel.write(pendingByte);
                    }
                    dstChannel.flush();
                    pendingBytes.clear();
                  }
                }
              }

              /*
              if (type == 1) {
                LOG.info("Sending data to {}, readBytes: {}, channel: {}, active: {}, open: {}", dst, remainingBytes,
                  dstChannel, dstChannel.isActive(), dstChannel.isOpen());
              }
              */

              dstChannel.writeAndFlush(bb);
            }

            remainingBytes = 0;
            status = Status.WAITING_HEADER1;
          } else {
            if (type == 1) {
              LOG.info("Remaining byte of {}... {} / {}", dst, remainingBytes, byteBuf.readableBytes());
            }
            return;
          }
          break;
        }
      }
    }
  }


  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    LOG.info("Removing channel inactive {}", ctx.channel().remoteAddress().toString());
    for (final Map.Entry<String, Channel> entry : taskChannelMap.entrySet()) {
      if (entry.getValue().equals(ctx.channel())) {
        LOG.info("Removing dst {}", entry.getKey());
        taskChannelMap.remove(entry.getKey());
        outputWriterFlusher.removeChannel(ctx.channel());
      }
    }
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    startToRelay(in, ctx);
  }

  final class ChannelFlush implements Flushable {

    private final Channel channel;

    public ChannelFlush(final Channel channel) {
      this.channel = channel;
    }

    @Override
    public void flush() throws IOException {
      channel.flush();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ChannelFlush that = (ChannelFlush) o;
      return Objects.equals(channel, that.channel);
    }

    @Override
    public int hashCode() {

      return Objects.hash(channel);
    }
  }
}
