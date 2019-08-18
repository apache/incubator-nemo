package org.apache.nemo.runtime.master;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.nemo.common.RendevousMessageEncoder;
import org.apache.nemo.common.RendevousResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RendevousServerDecoder extends MessageToMessageDecoder<ByteBuf> {

  private static final Logger LOG = LoggerFactory.getLogger(RendevousServerDecoder.class);

  private final ConcurrentMap<String, List<Channel>> dstRequestChannelMap;
  private final ConcurrentMap<String, Channel> rendevousChannelMap;
  private final ScheduledExecutorService scheduledExecutorService;

  public RendevousServerDecoder(final ConcurrentMap<String, List<Channel>> dstRequestChannelMap,
                                final ConcurrentMap<String, Channel> rendevousChannelMap,
                                final ScheduledExecutorService scheduledExecutorService) {
    this.dstRequestChannelMap = dstRequestChannelMap;
    this.rendevousChannelMap = rendevousChannelMap;
    this.scheduledExecutorService = scheduledExecutorService;
    scheduledExecutorService.scheduleAtFixedRate(() -> {

      for (final String dstRequestKey : dstRequestChannelMap.keySet()) {
        final List<Channel> channels = dstRequestChannelMap.get(dstRequestKey);

        synchronized (channels) {
          if (!channels.isEmpty() && rendevousChannelMap.containsKey(dstRequestKey) ) {

            //LOG.info("Sending response of {}", dstRequestKey);

            final Channel dst = rendevousChannelMap.get(dstRequestKey);
            // write

            channels.stream().forEach(channel -> {
              //LOG.info("Flush response {} to {}", dstRequestKey, channel);
              channel.writeAndFlush(new RendevousResponse(dstRequestKey,
                dst.remoteAddress().toString()));
            });

            channels.clear();
          }
        }
      }
    }, 1, 1, TimeUnit.SECONDS);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    LOG.info("Channel inactive {}", ctx.channel());

    final Iterator<Map.Entry<String, Channel>> iterator = rendevousChannelMap.entrySet().iterator();

    while (iterator.hasNext()) {
      final Map.Entry<String, Channel> entry = iterator.next();
      if (ctx.channel().equals(entry.getValue())) {
        iterator.remove();
      }
    }
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out) throws Exception {


    final RendevousMessageEncoder.Type type = RendevousMessageEncoder.Type.values()[byteBuf.readInt()];
    final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);

    switch (type) {
      case REQUEST: {
        final String dst = bis.readUTF();
        //LOG.info("Request dst {} from {}", dst, ctx.channel());

        dstRequestChannelMap.putIfAbsent(dst, new ArrayList<>());
        final List<Channel> channels = dstRequestChannelMap.get(dst);

        synchronized (channels) {
          channels.add(ctx.channel());
        }
        break;
      }
      case REGISTER: {
        // registering channel
        // send request
        final String dst = bis.readUTF();

        //LOG.info("Registering dst {} address {}", dst, ctx.channel());

        rendevousChannelMap.put(dst, ctx.channel());
        break;
      }
      default:
        throw new RuntimeException("Unsupported");
    }

  }
}
