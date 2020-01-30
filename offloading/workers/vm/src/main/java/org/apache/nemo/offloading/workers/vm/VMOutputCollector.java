package org.apache.nemo.offloading.workers.vm;

import io.netty.channel.Channel;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class VMOutputCollector implements OffloadingOutputCollector {
  private static final Logger LOG = LoggerFactory.getLogger(VMOutputCollector.class.getName());

  private final Channel channel;

  public VMOutputCollector(final Channel channel) {
    this.channel = channel;
  }

  @Override
  public void emit(Object output) {
    LOG.info("Output writing to {}/ {}", channel.remoteAddress(), output);
    channel.writeAndFlush(output);
  }
}
