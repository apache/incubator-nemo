package org.apache.nemo.runtime.master;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.OffloadingMasterEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class VMScalingWorker implements EventHandler<OffloadingMasterEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(VMScalingWorker.class.getName());

  private final String vmAddress;
  private final Channel channel;
  private final String executorId;
  private volatile boolean ready = false;
  private final Map<String, Pair<Double, Double>> executorCpuUseMap;

  public VMScalingWorker(final String vmAddress,
                         final Channel channel,
                         final ByteBuf initBuf,
                         final Map<String, Pair<Double, Double>> executorCpuUseMap) {
    this.vmAddress = vmAddress;
    this.channel = channel;
    this.executorId = RuntimeIdManager.generateExecutorId();
    this.executorCpuUseMap = executorCpuUseMap;

    final ByteBuf buf = initBuf.duplicate();

    final ByteBuf bb = ByteBufAllocator.DEFAULT.buffer(buf.writableBytes() + 30);

    bb.writeBytes(buf);

    LOG.info("Creating worker {}/{}..., sending init message", executorId, channel.remoteAddress());

    try {

      final ByteBufOutputStream bos = new ByteBufOutputStream(bb);
      bos.writeUTF(executorId);
      bos.close();
      channel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.CONNECT, bb));

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public String getVmAddress() {
    return vmAddress;
  }

  public String getExecutorId() {
    return executorId;
  }

  public boolean isReady() {
    return ready;
  }


  public void send(final OffloadingMasterEvent event) {
    LOG.info("Sending {} to {}", event.getType(), executorId);
    channel.writeAndFlush(event);
  }

  @Override
  public void onNext(OffloadingMasterEvent msg) {
    switch (msg.getType()) {
      case CONNECT_DONE: {
        LOG.info("Worker {}/{} is ready", executorId, channel.remoteAddress());
        ready = true;
        break;
      }
      case CPU_LOAD: {
        final double use = msg.getByteBuf().readDouble();
        msg.getByteBuf().release();
        LOG.info("CPU load of {}: {}", executorId, use);
        executorCpuUseMap.put(executorId,
          Pair.of(use, 0.0));
        break;
      }
    }
  }
}
