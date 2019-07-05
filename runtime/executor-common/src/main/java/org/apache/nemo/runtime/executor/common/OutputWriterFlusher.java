package org.apache.nemo.runtime.executor.common;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class OutputWriterFlusher {

  private static final Logger LOG = LoggerFactory.getLogger(OutputWriterFlusher.class.getName());

  private final long intervalMs;
  private final ScheduledExecutorService scheduledExecutorService;

  private final List<Channel> channelList;

  public OutputWriterFlusher(final long intervalMs) {
    this.intervalMs = intervalMs;
    this.scheduledExecutorService = Executors.newScheduledThreadPool(5);
    this.channelList = new ArrayList<>();

    scheduledExecutorService.scheduleAtFixedRate(() -> {

      synchronized (channelList) {
        final Iterator<Channel> iterator = channelList.iterator();

        while (iterator.hasNext()) {
          final Channel channel = iterator.next();

          if (channel.isActive()) {
            channel.flush();
          } else {
            LOG.info("Remove inactive channel {}", channel);
            iterator.remove();
          }
        }
      }

    }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
  }

  public void registerChannel(final Channel channel) {

    LOG.info("Registering channel {}", channel);
    synchronized (channelList) {
      channelList.add(channel);
    }
  }

  public void removeChannel(final Channel channel) {
    LOG.info("Remove channel {}", channel);

    synchronized (channelList) {
      channelList.remove(channel);
    }
  }
}
