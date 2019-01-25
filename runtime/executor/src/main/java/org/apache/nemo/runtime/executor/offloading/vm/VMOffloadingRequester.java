package org.apache.nemo.runtime.executor.offloading.vm;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.base64.Base64Decoder;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.NettyChannelInitializer;
import org.apache.nemo.common.lambda.LambdaStatus;
import org.apache.nemo.common.lambda.NettyLambdaInboundHandler;
import org.apache.nemo.runtime.executor.datatransfer.AWSUtils;
import org.apache.nemo.runtime.executor.offloading.NemoEventHandler;
import org.apache.nemo.runtime.executor.offloading.OffloadingRequester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.nemo.common.lambda.Constants.VM_WORKER_PORT;

public final class VMOffloadingRequester implements OffloadingRequester {
  private static final Logger LOG = LoggerFactory.getLogger(VMOffloadingRequester.class.getName());

  private final ScheduledExecutorService warmer = Executors.newSingleThreadScheduledExecutor();
  private final int warmupPeriod = 90; // sec
  private final AWSLambdaAsync awsLambda;
  private final NemoEventHandler nemoEventHandler;

  private final ExecutorService executorService = Executors.newFixedThreadPool(30);
  private final AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();

  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private EventLoopGroup serverBossGroup;
  private EventLoopGroup serverWorkerGroup;
  private Channel acceptor;
  private Map<Channel, EventHandler> channelEventHandlerMap;

  private final List<String> vmAddresses = Arrays.asList("172.31.37.143");
  private final List<String> instanceIds = Arrays.asList("i-0148d7ea6eae4cc80");

  private final String serverAddress;
  private final int serverPort;

  private final List<Channel> readyVMs = new LinkedList<>();

  private EventLoopGroup clientWorkerGroup;

  private final ConcurrentMap<Channel, EventHandler<NemoEvent>> map;

  private final AtomicBoolean stopped = new AtomicBoolean(true);

  int channelIndex = 0;


  /**
   * Netty client bootstrap.
   */
  private Bootstrap clientBootstrap;

  public VMOffloadingRequester(final NemoEventHandler nemoEventHandler,
                               final int poolSize,
                               final String serverAddress,
                               final int port) {
    this.awsLambda = AWSLambdaAsyncClientBuilder.standard().withClientConfiguration(
      new ClientConfiguration().withMaxConnections(500)).build();
    this.nemoEventHandler = nemoEventHandler;
    this.serverAddress = serverAddress;
    this.serverPort = port;

    this.clientWorkerGroup = new NioEventLoopGroup(1,
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
  public void createChannelRequest() {

    executorService.execute(() -> {
      if (stopped.compareAndSet(true, false)) {
        // 1 start instance
        final StartInstancesRequest request = new StartInstancesRequest()
          .withInstanceIds(instanceIds);
        LOG.info("Starting ec2 instances {}/{}", instanceIds, System.currentTimeMillis());
        ec2.startInstances(request);
        LOG.info("End of Starting ec2 instances {}/{}", instanceIds, System.currentTimeMillis());

        // 2 connect to the instance
        for (final String address : vmAddresses) {
          ChannelFuture channelFuture;
          while (true) {
            channelFuture = clientBootstrap.connect(new InetSocketAddress(address, VM_WORKER_PORT));
            channelFuture.awaitUninterruptibly(2000);
            assert channelFuture.isDone();
            if (!channelFuture.isSuccess()) {
              LOG.warn("A connection failed for " + address + "  waiting...");
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
      requestChannel.writeAndFlush(new NemoEvent(NemoEvent.Type.CLIENT_HANDSHAKE, bytes, bytes.length));

    });
  }

  @Override
  public void destroy() {
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
