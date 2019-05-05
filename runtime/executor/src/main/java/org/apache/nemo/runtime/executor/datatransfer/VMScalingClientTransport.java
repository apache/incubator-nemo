package org.apache.nemo.runtime.executor.datatransfer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.nemo.offloading.common.NettyChannelInitializer;
import org.apache.nemo.offloading.common.NettyLambdaInboundHandler;
import org.apache.nemo.offloading.common.OffloadingEvent;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class VMScalingClientTransport {

  private final Bootstrap clientBootstrap;
  private final EventLoopGroup clientWorkerGroup;
  private final ConcurrentMap<String, ChannelFuture> channelMap;

  @Inject
  private VMScalingClientTransport() {
              this.clientWorkerGroup = new NioEventLoopGroup(10,
        new DefaultThreadFactory("VMScalingClient"));
    this.clientBootstrap = new Bootstrap();
    this.channelMap = new ConcurrentHashMap<>();
    this.clientBootstrap.group(clientWorkerGroup)
        .channel(NioSocketChannel.class)
        .handler(new NettyChannelInitializer(new VMLambdaInboundHandler()))
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.SO_KEEPALIVE, true);
  }

  public synchronized ChannelFuture connectTo(final String address, final int port) {
    final String key = address + ":" + port;
    if (channelMap.containsKey(key)) {
      return channelMap.get(key);
    }

    final ChannelFuture channelFuture;
    channelFuture = clientBootstrap.connect(new InetSocketAddress(address, port));
    channelFuture.awaitUninterruptibly();
    assert channelFuture.isDone();
    if (!channelFuture.isSuccess()) {
      final StringBuilder sb = new StringBuilder("A connection failed at Source - ");
      sb.append(channelFuture.cause());
      throw new RuntimeException(sb.toString());
    }
    channelMap.put(key, channelFuture);
    return channelFuture;
  }

  final class VMLambdaInboundHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(
      final ChannelHandlerContext ctx, final Object msg) throws Exception {

    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
      cause.printStackTrace();
      System.out.println("Exception1!: " + cause.toString());
      ctx.close();
      //channelMap.remove(ctx.channel());
    }
  }
}
