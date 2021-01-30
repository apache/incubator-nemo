package org.apache.nemo.runtime.executor.task.util;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.codec.string.StringEncoder;
import org.apache.nemo.common.ir.Readable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public final class TCPSourceGenerator {

  public final List<Channel> channels;

  public static final int PORT = 12512;

  public TCPSourceGenerator(final int parallelism) {

    this.channels = new ArrayList<>(parallelism);

    EventLoopGroup bossGroup = new NioEventLoopGroup();	// (1)
    EventLoopGroup workerGroup = new NioEventLoopGroup(); // (2)
    try {
      ServerBootstrap b = new ServerBootstrap();	// (3)
      b.group(bossGroup, workerGroup)	// (4)
        .channel(NioServerSocketChannel.class) // (5)
        .childHandler(new SimpleTCPChannelInitializer()) // (6)
        .childOption(ChannelOption.SO_KEEPALIVE, true); // (7)

      // Bind and start to accept incoming connections.
      ChannelFuture f = b.bind(PORT).sync();	// (8)
      // f.channel().closeFuture().sync(); // (10)
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } finally {
      // workerGroup.shutdownGracefully(); // (11)
      // bossGroup.shutdownGracefully(); // (12)
    }
  }


  public void addEvent(final int index, final EventOrWatermark event) {
    channels.get(index).writeAndFlush(event);
  }


  public final class SimpleTCPChannelInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
      socketChannel.pipeline().addLast(new ObjectEncoder()); // (1)
      socketChannel.pipeline().addLast(new ObjectDecoder(ClassResolvers
        .cacheDisabled(getClass().getClassLoader())));// (2)
      socketChannel.pipeline().addLast(new SimpleTCPChannelHandler()); // (3)
    }
  }

  public class SimpleTCPChannelHandler extends SimpleChannelInboundHandler<String> {

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      synchronized (channels) {
        channels.add(ctx.channel());
      }
      System.out.print("Channel registered for readable " + ctx.channel().remoteAddress());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String s) throws Exception {

    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
    }
  }
}
