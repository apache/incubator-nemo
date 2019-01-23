package org.apache.nemo.runtime.executor.lambda;

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
import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.executor.datatransfer.AWSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class NettyServerLambdaTransport {
  private static final Logger LOG = LoggerFactory.getLogger(NettyServerLambdaTransport.class.getName());
  private static final int SERVER_BOSS_NUM_THREADS = 3;
  private static final int SERVER_WORKER_NUM_THREADS = 10;
  private static final String CLASS_NAME = NettyServerLambdaTransport.class.getName();
  private static final String ADDRESS = "172.31.6.35";
  private static final String PUBLIC_ADDRESS = "52.196.246.109";
  private static final int PORT = 20332;

  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private EventLoopGroup serverBossGroup;
  private EventLoopGroup serverWorkerGroup;
  private Channel acceptor;
  private NemoEventHandler nemoEventHandler;
  private Map<Channel, EventHandler> channelEventHandlerMap;
  private AWSLambdaAsync awsLambda;
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  public static final NettyServerLambdaTransport INSTANCE = new NettyServerLambdaTransport();

  private final ScheduledExecutorService warmer = Executors.newSingleThreadScheduledExecutor();

  private final int poolSize = 5;
  private final int warmupPeriod = 90; // sec

  private final AtomicBoolean initialized = new AtomicBoolean(false);

  private final List<Channel> channelPool;

  private NettyServerLambdaTransport() {
    LOG.info("Netty server lambda transport created");
    lazyInit();
    LOG.info("Netty server lambda transport created end");
    initialized.set(true);
    this.channelPool = new ArrayList<>(poolSize);


    warmer.scheduleAtFixedRate(() -> {
        //channelPool.clear();

        nemoEventHandler.getPendingRequest().addAndGet(poolSize);

        for (int i = 0; i < poolSize; i++) {
          executorService.submit(() -> {
            // Trigger lambdas
            final InvokeRequest request = new InvokeRequest()
              .withFunctionName(AWSUtils.SIDEINPUT_LAMBDA_NAME2)
              .withPayload(String.format("{\"address\":\"%s\", \"port\": %d}",
                PUBLIC_ADDRESS, PORT));
            return awsLambda.invokeAsync(request);
          });
        }

        // take
        for (int i = 0; i < poolSize; i++) {
          try {
            //channelPool.add(nemoEventHandler.getHandshakeQueue().take().left());
            nemoEventHandler.getHandshakeQueue().take().left();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        LOG.info("Warmup end");

        while (nemoEventHandler.getPendingRequest().getAndDecrement() > 0) {
          try {
            final Channel channel = nemoEventHandler.getReadyQueue().take().left();
            channel.writeAndFlush(new NemoEvent(NemoEvent.Type.WARMUP_END, new byte[0], 0));
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        nemoEventHandler.getPendingRequest().getAndIncrement();

        // send end event
      /*
        channelPool.forEach(channel -> {
          channel.writeAndFlush(new NemoEvent(NemoEvent.Type.WARMUP_END, new byte[0], 0));
        });
        */

    }, 0, warmupPeriod, TimeUnit.SECONDS);
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
    this.awsLambda = AWSLambdaAsyncClientBuilder.standard().withClientConfiguration(
      new ClientConfiguration().withMaxConnections(500)).build();
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

  public Future<Channel> createLambdaChannel(final List<String> serializedVertices) {
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

    executorService.submit(() -> {
      // Trigger lambdas
      if (nemoEventHandler.getPendingRequest().getAndDecrement() > 0) {
        return null;
      } else {
        // add 2 for the decrement and for the new request
        nemoEventHandler.getPendingRequest().addAndGet(2);
        final InvokeRequest request = new InvokeRequest()
          .withFunctionName(AWSUtils.SIDEINPUT_LAMBDA_NAME2)
          .withPayload(String.format("{\"address\":\"%s\", \"port\": %d}",
            PUBLIC_ADDRESS, PORT));
        return awsLambda.invokeAsync(request);
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
