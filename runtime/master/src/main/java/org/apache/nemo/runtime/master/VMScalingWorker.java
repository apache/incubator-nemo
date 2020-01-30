package org.apache.nemo.runtime.master;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.Channel;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.OffloadingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class VMScalingWorker implements EventHandler<OffloadingEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(VMScalingWorker.class.getName());

  private final Channel channel;
  private final String executorId;
  private volatile boolean ready = false;

  public VMScalingWorker(final Channel channel,
                         final ByteBuf initBuf) {
    this.channel = channel;
    this.executorId = RuntimeIdManager.generateExecutorId();

    final ByteBuf buf = initBuf.duplicate();

    final ByteBuf bb = ByteBufAllocator.DEFAULT.buffer(buf.writableBytes() + 30);

    bb.writeBytes(buf);

    LOG.info("Creating worker {}/{}..., sending init message", executorId, channel.remoteAddress());

    try {

      final ByteBufOutputStream bos = new ByteBufOutputStream(bb);
      bos.writeUTF(executorId);
      bos.close();
      channel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.CONNECT, bb));

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public boolean isReady() {
    return ready;
  }


  public void send(ByteBuf input) {

  }

  @Override
  public void onNext(OffloadingEvent msg) {
    switch (msg.getType()) {
      case CONNECT_DONE: {
        LOG.info("Worker {}/{} is ready", executorId, channel.remoteAddress());
        ready = true;
        break;
      }
    }
  }
}
