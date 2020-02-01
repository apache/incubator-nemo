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
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.executor.SFTaskMetrics;
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

  @Inject
  private VMScalingWorkerConnector(final SFTaskMetrics sfTaskMetrics) {
    this.clientWorkerGroup = new NioEventLoopGroup(10,
      new DefaultThreadFactory("hello" + "-ClientWorker"));
    this.clientBootstrap = new Bootstrap();
    this.map = new ConcurrentHashMap<>();
    this.executorChannelMap = new ConcurrentHashMap<>();
    this.lockMap = new ConcurrentHashMap<>();
    this.clientBootstrap.group(clientWorkerGroup)
      .channel(NioSocketChannel.class)
      .handler(new NettyChannelInitializer(new NettyLambdaInboundHandler(map)))
      .option(ChannelOption.SO_REUSEADDR, true)
      .option(ChannelOption.SO_KEEPALIVE, true);

    this.outputDecoder = new MiddleOffloadingOutputDecoder();
    this.sfTaskMetrics = sfTaskMetrics;
  }

  public Channel connectTo(final String executorId,
                           final String address,
                           final TaskExecutor te) {

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
      final VMScalingWorkerEventHandler eventHandler =
        new VMScalingWorkerEventHandler(te, sfTaskMetrics, executorId);

      LOG.info("Connecting to executor {} / {}", executorId, channel);

      map.put(channel, (msg) -> {
        // LOG.info("Receiving msg {} from {}", msg, channel);

        switch (msg.getType()) {
          case RESULT: {
            final ByteBufInputStream bis = new ByteBufInputStream(msg.getByteBuf());
            try {
              final Object data = outputDecoder.decode(msg.getByteBuf());
              eventHandler.onNext(data);
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
