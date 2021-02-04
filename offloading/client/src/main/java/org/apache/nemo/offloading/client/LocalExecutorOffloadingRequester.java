package org.apache.nemo.offloading.client;


import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.NettyChannelInitializer;
import org.apache.nemo.offloading.common.NettyLambdaInboundHandler;
import org.apache.nemo.offloading.common.OffloadingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nemo.offloading.common.Constants.VM_WORKER_PORT;

public final class LocalExecutorOffloadingRequester implements OffloadingRequester {

  private static final Logger LOG = LoggerFactory.getLogger(LocalExecutorOffloadingRequester.class.getName());

  private final String serverAddress;
  private final int serverPort;

  //private final List<Channel> readyVMs = new LinkedList<>();

  private EventLoopGroup clientWorkerGroup;

  private final ConcurrentMap<Channel, EventHandler<OffloadingEvent>> map;

  private final AtomicBoolean stopped = new AtomicBoolean(true);

  private final AtomicInteger requestId = new AtomicInteger(0);
  /**
   * Netty client bootstrap.
   */
  private Bootstrap clientBootstrap;

  // key: remoteAddress, value: instanceId
  private final Map<String, String> vmChannelMap = new ConcurrentHashMap<>();

  private final AtomicInteger numVMs = new AtomicInteger(0);
  private final ExecutorService waitingExecutor = Executors.newCachedThreadPool();

  public LocalExecutorOffloadingRequester(final String serverAddress,
                                          final int port) {
    this.serverAddress = serverAddress;
    this.serverPort = port;
    this.clientWorkerGroup = new NioEventLoopGroup(10,
      new DefaultThreadFactory("hello" + "-ClientWorker"));
    this.clientBootstrap = new Bootstrap();
    this.map = new ConcurrentHashMap<>();
    this.clientBootstrap.group(clientWorkerGroup)
      .channel(NioSocketChannel.class)
      .handler(new NettyChannelInitializer(new NettyLambdaInboundHandler(map)))
      .option(ChannelOption.SO_REUSEADDR, true)
      .option(ChannelOption.SO_KEEPALIVE, true);
  }

  @Override
  public void start() {
    // ping pong

  }

  @Override
  public synchronized void destroyChannel(final Channel channel) {
    final String addr = channel.remoteAddress().toString().split(":")[0];
    final String instanceId = vmChannelMap.remove(addr);
    numVMs.getAndDecrement();
    LOG.info("Stopping instance {}, channel: {}", instanceId, addr);
  }

  private final int port = new Random(System.currentTimeMillis()).nextInt(500)
   + VM_WORKER_PORT;

  private final AtomicInteger atomicInteger = new AtomicInteger(0);

  @Override
  public synchronized void createChannelRequest() {
    final int myPort = port + atomicInteger.getAndIncrement();
    LOG.info("Creating VM worker with port " + myPort);
    final String path = "/Users/taegeonum/Projects/CMS_SNU/incubator-nemo-lambda/offloading/workers/vm/target/offloading-vm-0.2-SNAPSHOT-shaded.jar";
      waitingExecutor.execute(() -> {
        try {
          Process p = Runtime.getRuntime().exec( "java -cp " + path + " org.apache.nemo.offloading.workers.vm.VMWorker " + myPort);

          String line;
          BufferedReader in = new BufferedReader(
            new InputStreamReader(p.getInputStream()) );

          BufferedReader stdError = new BufferedReader(new
            InputStreamReader(p.getErrorStream()));


          while (true) {
            while (in.ready() && (line = in.readLine()) != null) {
              System.out.println("[VMWworker " + myPort + "]: " + line);
            }
            // in.close();
            // LOG.info("End of read line !!!!!!!!!!!!!!!!!!!!");

            while (stdError.ready() && (line = stdError.readLine()) != null) {
              System.out.println("[VMWworker " + myPort + "]: " + line);
            }
            // stdError.close();

            try {
              Thread.sleep(300);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }

        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      });
      waitInstance(myPort);
    LOG.info("Create request at VMOffloadingREquestor");
  }


  private void waitInstance(final int myPort) {
    final long waitingTime = 2000;

    waitingExecutor.execute(() -> {
      ChannelFuture channelFuture;
      while (true) {
        final long st = System.currentTimeMillis();
        channelFuture = clientBootstrap.connect(new InetSocketAddress("localhost", myPort));
        channelFuture.awaitUninterruptibly(waitingTime);
        assert channelFuture.isDone();
        if (!channelFuture.isSuccess()) {
          LOG.warn("A connection failed for localhost  waiting...");
          final long elapsedTime = System.currentTimeMillis() - st;
          try {
            Thread.sleep(Math.max(1, waitingTime - elapsedTime));
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
        serverAddress, serverPort, requestId.getAndIncrement()).getBytes();
      openChannel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.SEND_ADDRESS, bytes, bytes.length));

      LOG.info("Add channel: {}, address: {}", openChannel, openChannel.remoteAddress());

      //return openChannel;
    });
  }


  @Override
  public void destroy() {
    /*
    synchronized (readyVMs) {
      readyVMs.clear();
    }
    */
    stopped.set(true);
  }

  @Override
  public void close() {

  }
}
