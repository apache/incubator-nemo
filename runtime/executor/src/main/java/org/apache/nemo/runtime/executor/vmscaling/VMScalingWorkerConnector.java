package org.apache.nemo.runtime.executor.vmscaling;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.nemo.common.Pair;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.executor.SFTaskMetrics;
import org.apache.nemo.runtime.executor.StageOffloadingWorkerManager;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.lambdaexecutor.middle.MiddleOffloadingOutputDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.nemo.offloading.common.Constants.VM_WORKER_PORT;
import static org.apache.nemo.offloading.common.OffloadingEvent.Type.CPU_LOAD;
import static org.apache.nemo.offloading.common.OffloadingEvent.Type.END;
import static org.apache.nemo.offloading.common.OffloadingEvent.Type.RESULT;

public final class VMScalingWorkerConnector {

  private static final Logger LOG = LoggerFactory.getLogger(VMScalingWorkerConnector.class.getName());

  /**
   * Netty client bootstrap.
   */
  private final Bootstrap clientBootstrap;
  private final EventLoopGroup clientWorkerGroup;
  private final ConcurrentMap<Channel, EventHandler<OffloadingEvent>> map;
  private final Map<String, Channel> executorChannelMap;
  private final Map<String, Object> lockMap;
  private final OffloadingDecoder outputDecoder;
  private final SFTaskMetrics sfTaskMetrics;
  private final Map<String, VMScalingWorkerEventHandler> taskEventHandlerMap;
  private final StageOffloadingWorkerManager stageOffloadingWorkerManager;

  @Inject
  private VMScalingWorkerConnector(
    final SFTaskMetrics sfTaskMetrics,
    final StageOffloadingWorkerManager stageOffloadingWorkerManager) {
    this.clientWorkerGroup = new NioEventLoopGroup(10,
      new DefaultThreadFactory("hello" + "-ClientWorker"));
    this.clientBootstrap = new Bootstrap();
    this.map = new ConcurrentHashMap<>();
    this.executorChannelMap = new ConcurrentHashMap<>();
    this.lockMap = new ConcurrentHashMap<>();
    this.taskEventHandlerMap = new ConcurrentHashMap<>();
    this.clientBootstrap.group(clientWorkerGroup)
      .channel(NioSocketChannel.class)
      .handler(new NettyChannelInitializer(new NettyLambdaInboundHandler(map)))
      .option(ChannelOption.SO_REUSEADDR, true)
      .option(ChannelOption.SO_KEEPALIVE, true);

    this.outputDecoder = new MiddleOffloadingOutputDecoder();
    this.sfTaskMetrics = sfTaskMetrics;
    this.stageOffloadingWorkerManager = stageOffloadingWorkerManager;
  }

  public Channel connectTo(final String executorId,
                           final String stageId,
                           final String address,
                           final TaskExecutor te) {

    taskEventHandlerMap.put(te.getId(),
      new VMScalingWorkerEventHandler(te, sfTaskMetrics,
        executorId, stageId, stageOffloadingWorkerManager));

    if (executorChannelMap.containsKey(executorId)) {
      return executorChannelMap.get(executorId);
    }

    lockMap.putIfAbsent(executorId, new Object());

    final Object lock = lockMap.get(executorId);
    synchronized (lock) {
      if (executorChannelMap.containsKey(executorId)) {
        return executorChannelMap.get(executorId);
      }

      final ChannelFuture channelFuture = clientBootstrap.connect(new InetSocketAddress(address, VM_WORKER_PORT));
      channelFuture.awaitUninterruptibly();
      final Channel channel = channelFuture.channel();
      executorChannelMap.putIfAbsent(executorId, channel);

      LOG.info("Connecting to executor {} / {}", executorId, channel);

      map.put(channel, (msg) -> {
        // LOG.info("Receiving msg {} from {}", msg, channel);

        switch (msg.getType()) {
          case RESULT: {
            final ByteBufInputStream bis = new ByteBufInputStream(msg.getByteBuf());
            try {
              final Pair<String, Object> data =
                (Pair<String, Object>) outputDecoder.decode(msg.getByteBuf());

              taskEventHandlerMap.get(data.left()).onNext(data.right());
              msg.getByteBuf().release();
            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException();
            }
            break;
          }
          case CPU_LOAD: {
            final double load = msg.getByteBuf().readDouble();
            LOG.info("Receive cpu load {}", load);
            break;
          }
          case END:
            if (Constants.enableLambdaLogging) {
              LOG.info("Receive end");
            }
            msg.getByteBuf().release();
            break;
          default:
            throw new RuntimeException("Invalid type: " + msg);
        }
      });

      return executorChannelMap.get(executorId);
    }
  }
}
