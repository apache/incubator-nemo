package org.apache.nemo.runtime.master;


import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
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
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.VMWorkerConf;
import org.apache.nemo.offloading.client.OffloadingEventHandler;
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
import java.util.concurrent.atomic.AtomicReference;

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

  //private final List<Channel> readyVMs = new LinkedList<>();

  private EventLoopGroup clientWorkerGroup;

  private final ConcurrentMap<Channel, EventHandler<OffloadingEvent>> map;

  private final AtomicBoolean stopped = new AtomicBoolean(true);


  private final AtomicInteger requestId = new AtomicInteger(0);

  /**
   * Netty client bootstrap.
   */
  private final Bootstrap clientBootstrap;

  private boolean vmStarted = false;

  private final ExecutorService createChannelExecutor;

  private final BlockingQueue<Integer> offloadingRequests = new LinkedBlockingQueue<>();

  //final OffloadingEvent requestEvent;

  private final AtomicInteger pendingRequests = new AtomicInteger(0);

  // value: (instanceId, address)
  // key: remoteAddress, value: instanceId
  private final Map<String, String> vmChannelMap = new ConcurrentHashMap<>();


  private final List<String> vmAddresses = VMScalingAddresses.VM_ADDRESSES;
  private final List<String> instanceIds = VMScalingAddresses.INSTANCE_IDS;

  private final AtomicInteger numVMs = new AtomicInteger(0);

  private final ExecutorService waitingExecutor = Executors.newCachedThreadPool();

  private final VMWorkerConf vmWorkerConf;
  private ByteBuf vmWorkerInitByteBuf;
  private final Map<String, Pair<Double, Double>> executorCpuUseMap;

  public VMOffloadingRequester(final OffloadingEventHandler nemoEventHandler,
                               final String serverAddress,
                               final int port,
                               final VMWorkerConf vmWorkerConf,
                               final Map<String, Pair<Double, Double>> executorCpuUseMap) {
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

    this.vmWorkerConf = vmWorkerConf;
    this.executorCpuUseMap = executorCpuUseMap;
  }


  public List<CompletableFuture<VMScalingWorker>> createWorkers(final int num) {
    final int currNum = numVMs.get();
    final List<String> addresses = vmAddresses.subList(currNum, currNum + num);
    final List<String> ids = instanceIds.subList(currNum, currNum + num);
    numVMs.addAndGet(num);

    if (vmWorkerInitByteBuf == null) {
      vmWorkerInitByteBuf = vmWorkerConf.encodeWithoutExecutorId();
    }

    return startInstances(ids, addresses);
  }

  public synchronized void destroyChannel(final Channel channel) {
    final String addr = channel.remoteAddress().toString().split(":")[0];
    final String instanceId = vmChannelMap.remove(addr);
    numVMs.getAndDecrement();
    LOG.info("Stopping instance {}, channel: {}", instanceId, addr);
    stopVM(instanceId);
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


  private List<CompletableFuture<VMScalingWorker>> startInstances(final List<String> instanceIds, final List<String> vmAddresses) {
    final StartInstancesRequest startRequest = new StartInstancesRequest()
      .withInstanceIds(instanceIds);
    ec2.startInstances(startRequest);
    LOG.info("Starting ec2 instances {}/{}", instanceIds, System.currentTimeMillis());

    final List<CompletableFuture<VMScalingWorker>> workers = new ArrayList<>(instanceIds.size());

    for (int i = 0; i < instanceIds.size(); i++) {
      final String vmAddress = vmAddresses.get(i);
      final String instanceId = instanceIds.get(i);

      workers.add(
        CompletableFuture.supplyAsync(() -> {
          ChannelFuture channelFuture;
          while (true) {
            final long st = System.currentTimeMillis();
            channelFuture = clientBootstrap.connect(new InetSocketAddress(vmAddress, VM_WORKER_PORT));
            channelFuture.awaitUninterruptibly(1000);
            assert channelFuture.isDone();
            if (!channelFuture.isSuccess()) {
              LOG.warn("A connection failed for " + vmAddress + "  waiting...");
              final long elapsedTime = System.currentTimeMillis() - st;
              try {
                Thread.sleep(Math.max(1, 1000 - elapsedTime));
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            } else {
              break;
            }
          }

          final Channel openChannel = channelFuture.channel();
          vmChannelMap.put(openChannel.remoteAddress().toString().split(":")[0], instanceId);

          final VMScalingWorker worker = new VMScalingWorker(vmAddress, openChannel, vmWorkerInitByteBuf,
            executorCpuUseMap);
          map.put(openChannel, worker);

          LOG.info("Open channel for VM: {}/{}, {}", vmAddress, instanceId, openChannel);
          return worker;
        })
      );

    }

    return workers;
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
