package org.apache.nemo.runtime.master;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.apache.nemo.common.RendevousMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public final class RendevousServerChannelInitializer extends ChannelInitializer<SocketChannel> {
  private static final Logger LOG = LoggerFactory.getLogger(RendevousServerChannelInitializer.class);

  private final ConcurrentMap<String, Channel> rendevousChannelMap;
  private final ConcurrentMap<String, List<Channel>> channelListMap;
  private final ScheduledExecutorService scheduledExecutorService;
  private final WatermarkManager watermarkManager;
  private final TaskScheduledMapMaster taskScheduledMap;

  public RendevousServerChannelInitializer(
    final ConcurrentMap<String, Channel> rendevousChannelMap,
    final WatermarkManager watermarkManager,
    final TaskScheduledMapMaster taskScheduledMap) {
    this.rendevousChannelMap = rendevousChannelMap;
    this.scheduledExecutorService = Executors.newScheduledThreadPool(5);
    this.channelListMap = new ConcurrentHashMap<>();
    this.watermarkManager = watermarkManager;
    this.taskScheduledMap = taskScheduledMap;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    LOG.info("Registering rendevous channel {}", ch.remoteAddress());
    // DO nothing!!

    ch.pipeline()
      .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
        Integer.MAX_VALUE, 0, 4, 0, 4))
      .addLast("frameEncoder", new LengthFieldPrepender(4))
      .addLast(new RendevousMessageEncoder())
      .addLast(new RendevousServerDecoder(
        channelListMap, rendevousChannelMap,
        scheduledExecutorService, watermarkManager, taskScheduledMap));
  }
}
