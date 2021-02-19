package org.apache.nemo.runtime.executor.common.datatransfer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class VMScalingClientTransport {
  private static final Logger LOG = LoggerFactory.getLogger(VMScalingClientTransport.class.getName());

  private final Bootstrap clientBootstrap;
  private final EventLoopGroup clientWorkerGroup;
  private final ConcurrentMap<String, ChannelFuture> channelMap;
  private final ConcurrentMap<String, AtomicInteger> channelCounterMap;

  public VMScalingClientTransport(final ChannelInitializer initializer) {
              this.clientWorkerGroup = new NioEventLoopGroup(10,
        new DefaultThreadFactory("VMScalingClient"));
    this.clientBootstrap = new Bootstrap();
    this.channelMap = new ConcurrentHashMap<>();
    this.channelCounterMap = new ConcurrentHashMap<>();
    this.clientBootstrap.group(clientWorkerGroup)
        .channel(NioSocketChannel.class)
        .handler(initializer)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.SO_KEEPALIVE, true);
  }

  public void close() {
    channelMap.values().forEach(channelFuture -> channelFuture.channel().close());
    clientWorkerGroup.shutdownGracefully();
  }

  public void disconnect(final String address, final int port) {
    final String key = address + ":" + port;
    final AtomicInteger counter = channelCounterMap.get(key);
    if (counter != null && counter.decrementAndGet() == 0) {
      // close channel
      LOG.info("Close channel... {}", address);
      channelMap.remove(key).channel().close().awaitUninterruptibly();
      channelCounterMap.remove(key);
    } else {
      LOG.info("Disconnect channel... {} / {}", address, counter);
    }
  }

  public synchronized ChannelFuture connectTo(final String address, final int port) {
    final String key = address + ":" + port;
    if (channelMap.containsKey(key)) {
      channelCounterMap.get(key).getAndIncrement();
      LOG.info("Get cached channel {}", address);
      return channelMap.get(key);
    }

    if (channelCounterMap.putIfAbsent(key, new AtomicInteger(1)) == null) {
      while (true) {
        final ChannelFuture channelFuture;
        channelFuture = clientBootstrap.connect(new InetSocketAddress(address, port));
        channelFuture.awaitUninterruptibly();
        assert channelFuture.isDone();
        if (!channelFuture.isSuccess()) {
          final StringBuilder sb = new StringBuilder("A connection failed at Source - ");
          sb.append(channelFuture.cause());
          throw new RuntimeException(sb.toString());
        }

        channelFuture.channel().flush();
        if (channelFuture.channel().isOpen()) {
          channelMap.put(key, channelFuture);
          LOG.info("Get new channel {} / {}", address, key);
          return channelFuture;
        }

        LOG.warn("Reconnect to {}/{}", address, port);
      }
    } else {
      LOG.info("Trying to get cached channel {} / {}", address, key);
      channelCounterMap.get(key).getAndIncrement();
      while (!channelMap.containsKey(key)) {
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      LOG.info("Get cached channel {}", address);
      return channelMap.get(key);
    }
  }
}
