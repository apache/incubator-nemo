package org.apache.nemo.runtime.master.lambda;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.NettyChannelInitializer;
import org.apache.nemo.offloading.common.NettyLambdaInboundHandler;
import org.apache.nemo.offloading.common.OffloadingMasterEvent;
import org.apache.nemo.runtime.master.RuntimeMaster;
import org.apache.reef.tang.InjectionFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nemo.offloading.common.Constants.VM_WORKER_PORT;

public final class LambdaYarnResourceRequester implements LambdaContainerRequester {

  private static final Logger LOG = LoggerFactory.getLogger(LambdaYarnResourceRequester.class.getName());

  private EventLoopGroup clientWorkerGroup;

  private final ConcurrentMap<Channel, EventHandler<OffloadingMasterEvent>> map;

  private final AtomicBoolean stopped = new AtomicBoolean(true);

  /**
   * Netty client bootstrap.
   */
  private Bootstrap clientBootstrap;

  // key: remoteAddress, value: instanceId
  private final Map<String, String> vmChannelMap = new ConcurrentHashMap<>();

  private final AtomicInteger numVMs = new AtomicInteger(0);
  private final ExecutorService waitingExecutor = Executors.newCachedThreadPool();

  private final InjectionFuture<RuntimeMaster> runtimeMasterInjectionFuture;

  @Inject
  public LambdaYarnResourceRequester(final InjectionFuture<RuntimeMaster> runtimeMasterInjectionFuture) {
    this.clientWorkerGroup = new NioEventLoopGroup(10,
      new DefaultThreadFactory("hello" + "-ClientWorker"));
    this.clientBootstrap = new Bootstrap();
    this.map = new ConcurrentHashMap<>();
    this.runtimeMasterInjectionFuture = runtimeMasterInjectionFuture;

    this.clientBootstrap.group(clientWorkerGroup)
      .channel(NioSocketChannel.class)
      .handler(new NettyChannelInitializer(new NettyLambdaInboundHandler(map)))
      .option(ChannelOption.SO_REUSEADDR, true)
      .option(ChannelOption.SO_KEEPALIVE, true);
  }

  private final int port = new Random(System.currentTimeMillis()).nextInt(500)
   + VM_WORKER_PORT;

  private final AtomicInteger atomicInteger = new AtomicInteger(0);

  @Override
  public void createRequest(String controlAddr,
                            int controlPort,
                            int requestId,
                            final String executorId) {
    final int myPort = port + atomicInteger.getAndIncrement();

    LOG.info("Creating VM worker with port for yarn " + myPort);

    final String key = executorId + "-offloading-" + myPort;

    runtimeMasterInjectionFuture.get()
      .requestOffloadingExecutor(myPort, key, executorId, (hostAddress) -> {
        LOG.info("Host address for " + key +  ": " + hostAddress);

        final long waitingTime = 1000;
        waitingExecutor.execute(() -> {
          ChannelFuture channelFuture;
          while (true) {
            final long st = System.currentTimeMillis();
            channelFuture = clientBootstrap.connect(new InetSocketAddress(hostAddress, myPort));
            channelFuture.awaitUninterruptibly(waitingTime);
            assert channelFuture.isDone();
            if (!channelFuture.isSuccess()) {
              LOG.warn("A connection failed for " + hostAddress + ":" + myPort + "  waiting...");
              final long elapsedTime = System.currentTimeMillis() - st;
              try {
                Thread.sleep(waitingTime);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            } else {
              break;
            }
          }

          final Channel openChannel = channelFuture.channel();
          LOG.info("Open channel for VM: {}", openChannel);

          // send handshake
          final byte[] bytes = String.format("{\"address\":\"%s\", \"port\": %d, \"requestId\": %d}",
            controlAddr, controlPort, requestId).getBytes();
          openChannel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.SEND_ADDRESS, bytes, bytes.length));

          LOG.info("Add channel: {}, address: {}", openChannel, openChannel.remoteAddress());
        });
      });
  }
}
