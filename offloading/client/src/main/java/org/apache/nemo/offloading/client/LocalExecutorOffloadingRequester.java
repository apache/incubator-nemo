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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nemo.offloading.common.Constants.VM_WORKER_PORT;

public final class LocalExecutorOffloadingRequester implements OffloadingRequester {

  private static final Logger LOG = LoggerFactory.getLogger(LocalExecutorOffloadingRequester.class.getName());

  private final OffloadingEventHandler nemoEventHandler;

  private final ExecutorService executorService = Executors.newFixedThreadPool(30);
  private final AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();

  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private EventLoopGroup serverBossGroup;
  private EventLoopGroup serverWorkerGroup;
  private Channel acceptor;
  private Map<Channel, EventHandler> channelEventHandlerMap;

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

  private boolean vmStarted = false;

  private final ExecutorService createChannelExecutor;

  private final BlockingQueue<Integer> offloadingRequests = new LinkedBlockingQueue<>();

  //final OffloadingEvent requestEvent;

  private final AtomicInteger pendingRequests = new AtomicInteger(0);
  private final int slotPerTask = 8;
  private int totalRequest = 0;


  private int handledRequestNum = 0;

  // value: (instanceId, address)
  // key: remoteAddress, value: instanceId
  private final Map<String, String> vmChannelMap = new ConcurrentHashMap<>();


  private List<String> vmAddresses;
  private List<String> instanceIds;

  private final AtomicInteger numVMs = new AtomicInteger(0);

  private final ExecutorService waitingExecutor = Executors.newCachedThreadPool();

  private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

  public LocalExecutorOffloadingRequester(final OffloadingEventHandler nemoEventHandler,
                                          final String serverAddress,
                                          final int port) {
    this.nemoEventHandler = nemoEventHandler;
    this.serverAddress = serverAddress;
    this.serverPort = port;
    this.createChannelExecutor = Executors.newSingleThreadExecutor();
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
    stopVM(instanceId);
  }

  @Override
  public synchronized void createChannelRequest() {
    LOG.info("Create request at VMOffloadingREquestor");

    final int index = numVMs.getAndIncrement();
     LOG.info("Request LocalExecutor!! {}", index);

    executorService.execute(() -> {
      try {
        startInstance();
      } catch (final Exception e) {
        e.printStackTrace();;
        throw new RuntimeException(e);
      }
    });
  }

  private void stopVM(final String instanceId) {
    while (true) {
      final DescribeInstancesRequest request = new DescribeInstancesRequest();
      request.setInstanceIds(Arrays.asList(instanceId));
      final DescribeInstancesResult response = ec2.describeInstances(request);

      for (final Reservation reservation : response.getReservations()) {
        for (final Instance instance : reservation.getInstances()) {
          if (instance.getInstanceId().equals(instanceId)) {
            if (instance.getState().getName().equals("running")) {
              // ready to stop
              final StopInstancesRequest stopRequest = new StopInstancesRequest()
                .withInstanceIds(instanceId);
              LOG.info("Stopping ec2 instances {}/{}", instanceId, System.currentTimeMillis());
              ec2.stopInstances(stopRequest);
              LOG.info("End of Stop ec2 instances {}/{}", instanceId, System.currentTimeMillis());
              return;
            } else if (instance.getState().getName().equals("pending")) {
              // waiting...
              try {
                Thread.sleep(2000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            } else {
              throw new RuntimeException("Unsupported state type: " + instance.getState().getName());
            }
          }
        }
      }
    }
  }

  private final List<String> requestInstanceIds = new ArrayList<>();

  private void startInstance() {
    final long waitingTime = 2000;

  }

  @Override
  public void destroy() {
    /*
    synchronized (readyVMs) {
      readyVMs.clear();
    }
    */
    LOG.info("Stopping instances {}", instanceIds);
    final StopInstancesRequest request = new StopInstancesRequest()
      .withInstanceIds(instanceIds);
    ec2.stopInstances(request);
    stopped.set(true);
  }

  private void startAndStop() {

  }

  private void createAndDestroy() {

  }

  @Override
  public void close() {

  }
}
