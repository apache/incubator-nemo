package org.apache.nemo.offloading.workers.vm;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import org.apache.nemo.offloading.common.OffloadingEncoder;
import org.apache.nemo.offloading.common.OffloadingEvent;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.apache.nemo.runtime.lambdaexecutor.middle.MiddleOffloadingOutputEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class VMOutputCollector implements OffloadingOutputCollector {
  private static final Logger LOG = LoggerFactory.getLogger(VMOutputCollector.class.getName());

  private final Channel channel;
  private final OffloadingEncoder encoder;

  public VMOutputCollector(final Channel channel) {
    this.channel = channel;
    this.encoder = new MiddleOffloadingOutputEncoder();
  }

  @Override
  public void emit(Object output) {
    final ByteBuf byteBuf = channel.alloc().buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);

    try {
      encoder.encode(output, bos);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    LOG.info("Output writing to {}/ {}", channel.remoteAddress(), output);

    channel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.RESULT, byteBuf));
  }
}
