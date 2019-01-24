package org.apache.nemo.runtime.executor.offloading;

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
import org.apache.nemo.runtime.executor.offloading.lambda.LambdaOffloadingRequester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class NettyServerTransport {
  private static final Logger LOG = LoggerFactory.getLogger(NettyServerTransport.class.getName());
  private static final int SERVER_BOSS_NUM_THREADS = 3;
  private static final int SERVER_WORKER_NUM_THREADS = 10;
  private static final String CLASS_NAME = NettyServerTransport.class.getName();
  private static final String ADDRESS = "172.31.6.35";
  private static final String PUBLIC_ADDRESS = "54.250.247.241";
  private static final int PORT = 20332;

  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private EventLoopGroup serverBossGroup;
  private EventLoopGroup serverWorkerGroup;
  private Channel acceptor;
  private NemoEventHandler nemoEventHandler;
  private Map<Channel, EventHandler> channelEventHandlerMap;

  private final ExecutorService executorService = Executors.newCachedThreadPool();

  public static final NettyServerTransport INSTANCE = new NettyServerTransport();

  private final int poolSize = 200;
  private final AtomicBoolean initialized = new AtomicBoolean(false);

  private final List<Channel> channelPool;

  private final OffloadingRequester offloadingRequester;

  private NettyServerTransport() {
    lazyInit();
    LOG.info("Netty server lambda transport created end");
    initialized.set(true);
    this.channelPool = new ArrayList<>(poolSize);
    this.offloadingRequester = new LambdaOffloadingRequester(
      nemoEventHandler, poolSize, PUBLIC_ADDRESS, PORT);
  }

  private void lazyInit() {
    this.serverBossGroup = new NioEventLoopGroup(SERVER_BOSS_NUM_THREADS,
      new DefaultThreadFactory(CLASS_NAME + "SourceServerBoss"));
    this.serverWorkerGroup = new NioEventLoopGroup(SERVER_WORKER_NUM_THREADS,
      new DefaultThreadFactory(CLASS_NAME + "SourceServerWorker"));
    this.channelEventHandlerMap = new ConcurrentHashMap<>();
    this.nemoEventHandler = new NemoEventHandler(channelEventHandlerMap);
    //this.awsLambda = AWSLambdaClientBuilder.standard().withClientConfiguration(
    //  new ClientConfiguration().withMaxConnections(150)).build();

    final ServerBootstrap serverBootstrap = new ServerBootstrap();
    serverBootstrap.group(this.serverBossGroup, this.serverWorkerGroup)
      .channel(NioServerSocketChannel.class)
      .childHandler(new NettyChannelInitializer(
        new NettyServerSideChannelHandler(serverChannelGroup, nemoEventHandler)))
      .option(ChannelOption.SO_BACKLOG, 128)
      .option(ChannelOption.SO_REUSEADDR, true)
      .childOption(ChannelOption.SO_KEEPALIVE, true);
    try {
      this.acceptor = serverBootstrap.bind(
        new InetSocketAddress(ADDRESS, PORT)).sync().channel();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public Object takeResult() {
    try {
      return nemoEventHandler.getResultQueue().take();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void setChannelHandler(final Channel channel, final EventHandler eventHandler) {
    channelEventHandlerMap.put(channel, eventHandler);
  }

  public Future<Channel> createOffloadingChannel(final List<String> serializedVertices) {
    if (initialized.compareAndSet(false, true)) {
      lazyInit();
    }

    /*
    final StringBuilder sb = new StringBuilder("");
    for (final String serializedVertex : serializedVertices) {
      sb.append("\"");
      sb.append(serializedVertex);
      sb.append("\"");
      if (serializedVertices.indexOf(serializedVertex) + 1 < serializedVertices.size()) {
        sb.append(",");
      }
      sb.append("]");
    }
    */

    System.out.println("Serialized vertices: " + serializedVertices.size());

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = null;
    try {
      oos = new ObjectOutputStream(bos);
      oos.writeObject(serializedVertices);
      oos.close();
      bos.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    byte[] serializedVerticesBytes = bos.toByteArray();
    System.out.println("Serialized vertices size: " + serializedVerticesBytes.length);

    executorService.execute(() -> {
      // Trigger lambdas
      if (nemoEventHandler.getPendingRequest().getAndDecrement() <= 0) {
        // add 2 for the decrement and for the new channel request
        nemoEventHandler.getPendingRequest().addAndGet(2);
        offloadingRequester.createChannelRequest();
      }
    });

    return new Future<Channel>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return false;
      }

      @Override
      public Channel get() throws InterruptedException, ExecutionException {
        try {
          final Channel channel = nemoEventHandler.getReadyQueue().take().left();
          channel.writeAndFlush(new NemoEvent(NemoEvent.Type.VERTICES,
            serializedVerticesBytes, serializedVerticesBytes.length));
          return channel;
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }

      @Override
      public Channel get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return get();
      }
    };
  }


}
