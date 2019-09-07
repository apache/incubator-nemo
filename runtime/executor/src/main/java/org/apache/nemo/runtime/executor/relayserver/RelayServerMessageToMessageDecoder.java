package org.apache.nemo.runtime.executor.relayserver;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.nemo.runtime.executor.common.OutputWriterFlusher;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class RelayServerMessageToMessageDecoder extends MessageToMessageDecoder<ByteBuf> {
  private static final Logger LOG = LoggerFactory.getLogger(RelayServerMessageToMessageDecoder.class);

  private final ConcurrentMap<String, Channel> taskChannelMap;
  private final ConcurrentMap<String, List<ByteBuf>> pendingByteMap;
  private final ScheduledExecutorService pendingFlusher;
  private final OutputWriterFlusher outputWriterFlusher;

  public RelayServerMessageToMessageDecoder(final ConcurrentMap<String, Channel> taskChannelMap,
                                            final ConcurrentMap<String, List<ByteBuf>> pendingByteMap,
                                            final OutputWriterFlusher outputWriterFlusher,
                                            final ScheduledExecutorService scheduledExecutorService) {
    this.taskChannelMap = taskChannelMap;
    this.pendingByteMap = pendingByteMap;
    this.pendingFlusher = scheduledExecutorService;
    this.outputWriterFlusher = outputWriterFlusher;

    pendingFlusher.scheduleAtFixedRate(() -> {
      try {

        for (final String dstKey : taskChannelMap.keySet()) {
          if (pendingByteMap.containsKey(dstKey)) {
            // 여기서 remove를 안해주는 이유
            // remove를 하면 순서가 꼬일수가 있음!!
            final List<ByteBuf> pendingBytes = pendingByteMap.get(dstKey);
            final Channel channel = taskChannelMap.get(dstKey);

            if (pendingBytes != null) {
              synchronized (pendingBytes) {
                LOG.info("Flushing pending byte {} size: {} / {}", dstKey, pendingBytes.size(), channel);
                for (final ByteBuf pendingByte : pendingBytes) {
                  if (channel.isOpen()) {
                    channel.write(pendingByte);
                  } else {
                    throw new RuntimeException("Channel closed ");
                  }
                }
                channel.flush();
                pendingBytes.clear();

                // 여기서 remove
                pendingByteMap.remove(dstKey);
              }
            }
          }
        }

        if (pendingByteMap.size() > 0) {
          LOG.info("Pending dst ... {}", pendingByteMap.keySet());
        }

      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

    }, 100, 100, TimeUnit.MILLISECONDS);
  }


  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    LOG.info("Removing channel inactive {}", ctx.channel().remoteAddress().toString());
    outputWriterFlusher.removeChannel(ctx.channel());
    for (final Map.Entry<String, Channel> entry : taskChannelMap.entrySet()) {
      if (entry.getValue().equals(ctx.channel())) {
        LOG.info("Removing dst {}", entry.getKey());

        if (pendingByteMap.containsKey(entry.getKey())) {
          throw new RuntimeException("Channel inactive.. but there are still pending bytes");
        }

        taskChannelMap.remove(entry.getKey());
      }
    }
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out) throws Exception {
    //startToRelay(in, ctx);

    byteBuf.retain();
    final char type = byteBuf.readChar();

    final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
    final String dst = bis.readUTF();

    //LOG.info("Dst: {}, readable: {}", dst, byteBuf.readableBytes());

    if (type == 0 || type == 1) {
      // data frame and control frame
      //final int maxRead = Math.min(remainingBytes, byteBuf.readableBytes());
      //final ByteBuf bb = byteBuf.readRetainedSlice(maxRead);

      if (!taskChannelMap.containsKey(dst)) {
        //LOG.info("Pending for dst {}... readable: {}", dst, byteBuf.readableBytes());
        pendingByteMap.putIfAbsent(dst, new ArrayList<>());
        final List<ByteBuf> pendingBytes = pendingByteMap.get(dst);

        if (pendingBytes != null) {

          synchronized (pendingBytes) {
            // 한번더 check
            // remove 되었을 수도 잇음 (모두 flush 되었을수도)
            if (pendingByteMap.containsKey(dst)) {
              pendingBytes.add(byteBuf);
              //LOG.info("Add {} byte to pendingBytes... size: {}", pendingBytes.size());
            } else {
              final Channel dstChannel = taskChannelMap.get(dst);
              if (dstChannel.isOpen()) {
                dstChannel.write(byteBuf);
              } else {
                throw new RuntimeException("Channel closed");
              }
            }
          }

        } else {

          final Channel dstChannel = taskChannelMap.get(dst);

          if (dstChannel.isOpen()) {
            dstChannel.write(byteBuf);
          } else {
            throw new RuntimeException("Channel closed");
          }
        }
      } else {

        final Channel dstChannel = taskChannelMap.get(dst);

        if (pendingByteMap.containsKey(dst)) {
          final List<ByteBuf> pendingBytes = pendingByteMap.get(dst);

          if (pendingBytes != null) {

            synchronized (pendingBytes) {
              if (pendingByteMap.containsKey(dst)) {
                LOG.info("Flushing pending byte {} size: {} / {}", dst, pendingBytes.size(), dstChannel);
                for (final ByteBuf pendingByte : pendingBytes) {
                  if (dstChannel.isOpen()) {
                    dstChannel.write(pendingByte);
                  } else {
                    throw new RuntimeException("Channel closed");
                  }
                }
                dstChannel.flush();
                pendingBytes.clear();

                pendingByteMap.remove(dst);
              }
            }

          }
        }

        dstChannel.write(byteBuf);
      }

    } else if (type == 2) {
      // control message
      final RelayControlMessage.Type controlMsgType = RelayControlMessage.Type.values()[byteBuf.readInt()];

      switch (controlMsgType) {
        case REGISTER: {
          LOG.info("Registering {} / {}", dst, ctx.channel());

          if (taskChannelMap.containsKey(dst)) {
            LOG.info("Duplicate dst channel in relayServer.. just overwrite {}", dst);
          }

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
}

