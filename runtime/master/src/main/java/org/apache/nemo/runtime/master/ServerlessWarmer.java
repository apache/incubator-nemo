package org.apache.nemo.runtime.master;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.NettyChannelInitializer;
import org.apache.nemo.common.lambda.Constants;
import org.apache.nemo.runtime.common.NetworkUtils;
import org.apache.nemo.runtime.common.offloading.NemoEventHandler;
import org.apache.nemo.runtime.common.offloading.NettyServerSideChannelHandler;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.nemo.common.lambda.Constants.POOL_SIZE;

public final class ServerlessWarmer {
  private static final Logger LOG = LoggerFactory.getLogger(ServerlessWarmer.class.getName());
  private static final int SERVER_BOSS_NUM_THREADS = 3;
  private static final int SERVER_WORKER_NUM_THREADS = 10;
  private static final String CLASS_NAME = ServerlessWarmer.class.getName();

  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private EventLoopGroup serverBossGroup;
  private EventLoopGroup serverWorkerGroup;
  private Channel acceptor;

  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private int port;
  private String localAddress;
  private final String publicAddress;
  private final NemoEventHandler nemoEventHandler;


  private final AWSLambdaAsync awsLambda;

  @Inject
  private ServerlessWarmer(final TcpPortProvider tcpPortProvider) {
    LOG.info("Start serverless warmer");
    this.serverBossGroup = new NioEventLoopGroup(SERVER_BOSS_NUM_THREADS,
      new DefaultThreadFactory(CLASS_NAME + "SourceServerBoss"));
    this.serverWorkerGroup = new NioEventLoopGroup(SERVER_WORKER_NUM_THREADS,
      new DefaultThreadFactory(CLASS_NAME + "SourceServerWorker"));

    final ServerBootstrap serverBootstrap = new ServerBootstrap();
    this.nemoEventHandler = new NemoEventHandler();
    serverBootstrap.group(this.serverBossGroup, this.serverWorkerGroup)
      .channel(NioServerSocketChannel.class)
      .childHandler(new NettyChannelInitializer(
        new NettyServerSideChannelHandler(serverChannelGroup, nemoEventHandler)))
      .option(ChannelOption.SO_BACKLOG, 128)
      .option(ChannelOption.SO_REUSEADDR, true)
      .childOption(ChannelOption.SO_KEEPALIVE, true);

    this.port = setUpRandomPortNettyServer(serverBootstrap, tcpPortProvider);
    this.publicAddress = NetworkUtils.getPublicIP();
    LOG.info("Public address: {}, localAddress: {}, port: {}", publicAddress, localAddress, port);
    LOG.info("Acceptor open: {}, active: {}", acceptor.isOpen(), acceptor.isActive());

    this.awsLambda = AWSLambdaAsyncClientBuilder.standard().withClientConfiguration(
      new ClientConfiguration().withMaxConnections(500)).build();
  }

  public void start() {

    LOG.info("Warm up start");
    for (int i = 0; i < POOL_SIZE; i++) {
      executorService.submit(() -> {
        // Trigger lambdas
        final InvokeRequest request = new InvokeRequest()
          .withFunctionName(Constants.SIDEINPUT_LAMBDA_NAME2)
          .withPayload(String.format("{\"address\":\"%s\", \"port\": %d}",
            publicAddress, port));
        return awsLambda.invokeAsync(request);
      });
    }

    // take
    for (int i = 0; i < POOL_SIZE; i++) {
      try {
        //channelPool.add(nemoEventHandler.getHandshakeQueue().take().left());
        nemoEventHandler.getHandshakeQueue().take().left();
        final Channel channel = nemoEventHandler.getReadyQueue().take().left();
        channel.writeAndFlush(new NemoEvent(NemoEvent.Type.WARMUP_END, new byte[0], 0));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public int getPort() {
    return port;
  }

  private int setUpRandomPortNettyServer(final ServerBootstrap serverBootstrap,
                                         final TcpPortProvider tcpPortProvider) {
    try {
      final InetAddress addr = NetworkUtils.getLocalHostLANAddress();
      final Iterator<Integer> portIterator = tcpPortProvider.iterator();
      while (true) {
        try {
          final int p = portIterator.next();
          localAddress = addr.getHostAddress();
          this.acceptor = serverBootstrap.bind(
            new InetSocketAddress(localAddress, port)).sync().channel();
          port = p;
          LOG.info("Server address: {}, Assigned server port = {}", localAddress, port);
          return port;
        } catch (final Exception e) {
          e.printStackTrace();
          LOG.info("Server address: {}, port: {}", localAddress, port);
          LOG.warn("Duplicate port is assigned to server... try again...");
        }
      }
    } catch (UnknownHostException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
