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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nemo.offloading.common.Constants.VM_WORKER_PORT;

public final class VMOffloadingRequester {

  private static final Logger LOG = LoggerFactory.getLogger(VMOffloadingRequester.class.getName());

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

  private final List<Channel> readyVMs = new LinkedList<>();

  private EventLoopGroup clientWorkerGroup;

  private final ConcurrentMap<Channel, EventHandler<OffloadingEvent>> map;

  private final AtomicBoolean stopped = new AtomicBoolean(true);

  private final List<String> vmAddresses = Arrays.asList("172.31.16.202");
  private final List<String> instanceIds = Arrays.asList("i-0707e910d42ab99fb");

  /**
   * Netty client bootstrap.
   */
  private Bootstrap clientBootstrap;

  private boolean vmStarted = false;

  private final ExecutorService createChannelExecutor;

  private final BlockingQueue<Integer> offloadingRequests = new LinkedBlockingQueue<>();

  final OffloadingEvent requestEvent;

  private int vmIndex = 0;
  private final AtomicInteger pendingRequests = new AtomicInteger(0);

  public VMOffloadingRequester(final OffloadingEventHandler nemoEventHandler,
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
    final byte[] bytes = String.format("{\"address\":\"%s\", \"port\": %d}",
      serverAddress, serverPort).getBytes();
    this.requestEvent = new OffloadingEvent(OffloadingEvent.Type.CLIENT_HANDSHAKE, bytes, bytes.length);

    createChannelExecutor.execute(() -> {
      while (true) {
        try {
          synchronized (readyVMs) {
            LOG.info("Pending requests...: {}", pendingRequests.get());
            if (!readyVMs.isEmpty()) {
              final Integer offloadingRequest = offloadingRequests.take();
              pendingRequests.getAndDecrement();
              final Channel openChannel = readyVMs.get(vmIndex);
              openChannel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.CLIENT_HANDSHAKE, bytes, bytes.length));
              vmIndex = (vmIndex + 1) % readyVMs.size();
            } else {
              Thread.sleep(1000);
            }
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    });
  }

  public void start() {
    // ping pong

  }

  public synchronized void createChannelRequest() {
    LOG.info("Create request at VMOffloadingREquestor");
    final long waitingTime = 2000;

    pendingRequests.getAndIncrement();
    offloadingRequests.add(1);

    if (vmStarted) {
      return;
    }

    vmStarted = true;


    executorService.execute(() -> {
      if (stopped.compareAndSet(true, false)) {
        // 1 start instance
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        request.setInstanceIds(instanceIds);
        DescribeInstancesResult response = ec2.describeInstances(request);

        for(final Reservation reservation : response.getReservations()) {
          for(final Instance instance : reservation.getInstances()) {
            while (true) {
              if (instance.getState().getName().equals("stopped")) {
                // ready to start
                final StartInstancesRequest startRequest = new StartInstancesRequest()
                  .withInstanceIds(instanceIds);
                LOG.info("Starting ec2 instances {}/{}", instanceIds, System.currentTimeMillis());
                ec2.startInstances(startRequest);
                LOG.info("End of Starting ec2 instances {}/{}", instanceIds, System.currentTimeMillis());
                break;
              } else if (instance.getState().getName().equals("stopping")) {
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

        // 2 connect to the instance
        for (final String address : vmAddresses) {
          ChannelFuture channelFuture;
          while (true) {
            final long st = System.currentTimeMillis();
            channelFuture = clientBootstrap.connect(new InetSocketAddress(address, VM_WORKER_PORT));
            channelFuture.awaitUninterruptibly(waitingTime);
            assert channelFuture.isDone();
            if (!channelFuture.isSuccess()) {
              LOG.warn("A connection failed for " + address + "  waiting...");
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
          synchronized (readyVMs) {
            readyVMs.add(openChannel);
          }
          LOG.info("Add channel: {}", openChannel);
        }
      }

      /*
      Channel requestChannel;

      while (true) {
        synchronized (readyVMs) {
          LOG.info("ReadyVM: {} ", readyVMs);
          if (readyVMs.size() > 0) {
            final int idx = (channelIndex + 1) % readyVMs.size();
            requestChannel = readyVMs.get(idx);
            break;
          }
        }

        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      LOG.info("Request to Channel {}", requestChannel);
      final byte[] bytes = String.format("{\"address\":\"%s\", \"port\": %d}",
        serverAddress, serverPort).getBytes();
      requestChannel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.CLIENT_HANDSHAKE, bytes, bytes.length));
      */

    });
  }

  public void destroy() {
    synchronized (readyVMs) {
      readyVMs.clear();
    }
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
