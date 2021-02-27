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
import org.apache.nemo.offloading.common.OffloadingMasterEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nemo.offloading.common.Constants.VM_WORKER_PORT;

public final class VMPoolRequester {

  private static final Logger LOG = LoggerFactory.getLogger(VMPoolRequester.class.getName());

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

  private final ConcurrentMap<Channel, EventHandler<OffloadingMasterEvent>> map;

  private final AtomicBoolean stopped = new AtomicBoolean(true);

  private final List<String> vmAddresses = Arrays.asList("172.31.16.202", "172.31.17.96", "172.31.18.133", "172.31.25.250", "172.31.21.223");
  private final List<String> instanceIds = Arrays.asList("i-0707e910d42ab99fb", "i-081f578c165a41a7a", "i-0d346bd15aed1a33f", "i-0756c588bf6b60a71", "i-09355b96aac481c5d");

  private final AtomicInteger requestId = new AtomicInteger(0);

  /**
   * Netty client bootstrap.
   */
  private Bootstrap clientBootstrap;

  private boolean vmStarted = false;

  private final ExecutorService createChannelExecutor;

  private final BlockingQueue<Integer> offloadingRequests = new LinkedBlockingQueue<>();

  //final OffloadingMasterEvent requestEvent;

  private final AtomicInteger pendingRequests = new AtomicInteger(0);
  private final int slotPerTask = 8;
  private int totalRequest = 0;


  private int handledRequestNum = 0;

  // value: (instanceId, address)
  // key: remoteAddress, value: instanceId
  private final Map<String, String> vmChannelMap = new ConcurrentHashMap<>();


  public VMPoolRequester(final OffloadingEventHandler nemoEventHandler,
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


    /*
    createChannelExecutor.execute(() -> {
      while (true) {
        try {
            LOG.info("Pending requests...: {}", pendingRequests.get());
            if (!readyVMs.isEmpty()) {
              final Integer offloadingRequest = offloadingRequests.take();
              if (handledRequestNum / slotPerTask < readyVMs.size()) {
                //final int vmIndex = handledRequestNum / slotPerTask;
                final int vmIndex = handledRequestNum % readyVMs.size();
                handledRequestNum += 1;
                pendingRequests.getAndDecrement();
                final Channel openChannel = readyVMs.get(vmIndex);
                openChannel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.CLIENT_HANDSHAKE, bytes, bytes.length));
              } else {
                offloadingRequests.add(1);
                Thread.sleep(1000);
              }
            } else {
              Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    });
    */


    // vm pool 쓸때 approach
    LOG.info("Create request at VMOffloadingREquestor");

    final int index = vmChannelMap.size();
    LOG.info("Request VM!! {}", index);

    executorService.execute(() -> {
      try {
        startInstance(instanceIds.get(index), vmAddresses.get(index));
      } catch (final Exception e) {
        e.printStackTrace();;
        throw new RuntimeException(e);
      }
    });
  }

  public void start() {
    // ping pong

  }

  public synchronized void destroyChannel(final Channel channel) {
    final String addr = channel.remoteAddress().toString().split(":")[0];
    final String instanceId = vmChannelMap.remove(addr);
    LOG.info("Stopping instance {}, channel: {}", instanceId, addr);
    //stopVM(instanceId);
  }

  public synchronized void createChannelRequest() {
    LOG.info("Create request at VMOffloadingREquestor");

    final int index = vmChannelMap.size();
     LOG.info("Request VM!! {}", index);

    executorService.execute(() -> {
      try {
        startInstance(instanceIds.get(index), vmAddresses.get(index));
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

  private void startVM(final String instanceId) {
    while (true) {
      final DescribeInstancesRequest request = new DescribeInstancesRequest();
      request.setInstanceIds(Arrays.asList(instanceId));
      final DescribeInstancesResult response = ec2.describeInstances(request);

      for(final Reservation reservation : response.getReservations()) {
        for(final Instance instance : reservation.getInstances()) {
          if (instance.getInstanceId().equals(instanceId)) {
            if (instance.getState().getName().equals("stopped")) {
              // ready to start
              final StartInstancesRequest startRequest = new StartInstancesRequest()
                .withInstanceIds(instanceId);
              LOG.info("Starting ec2 instances {}/{}", instanceId, System.currentTimeMillis());
              ec2.startInstances(startRequest);
              LOG.info("End of Starting ec2 instances {}/{}", instanceId, System.currentTimeMillis());
              return;
            } else if (instance.getState().getName().equals("stopping")) {
              // waiting...
              try {
                Thread.sleep(2000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            } else {
              LOG.warn("ec2 instance is currently running {}", instanceId);
              throw new RuntimeException("Unsupported state type: " + instance.getState().getName());
            }
          }
        }
      }
    }
  }

  private Channel startInstance(final String instanceId, final String vmAddress) {
    final long waitingTime = 2000;

    startVM(instanceId);

    ChannelFuture channelFuture;
    while (true) {
      final long st = System.currentTimeMillis();
      channelFuture = clientBootstrap.connect(new InetSocketAddress(vmAddress, VM_WORKER_PORT));
      channelFuture.awaitUninterruptibly(waitingTime);
      assert channelFuture.isDone();
      if (!channelFuture.isSuccess()) {
        LOG.warn("A connection failed for " + vmAddress + "  waiting...");
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
    openChannel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.CLIENT_HANDSHAKE, bytes, bytes.length));

    /*
    synchronized (readyVMs) {
      readyVMs.add(openChannel);
    }
    */

    LOG.info("Add channel: {}, address: {}", openChannel, openChannel.remoteAddress());

    vmChannelMap.put(openChannel.remoteAddress().toString().split(":")[0], instanceId);

    return openChannel;
  }

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

  public void close() {

  }
}
