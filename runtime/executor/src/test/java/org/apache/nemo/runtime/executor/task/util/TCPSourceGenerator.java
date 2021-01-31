package org.apache.nemo.runtime.executor.task.util;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.apache.nemo.common.Pair;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public final class TCPSourceGenerator {

  public final Channel[] channels;
  public final List<List<EventOrWatermark>> events;

  public static final int PORT = 12512;


  public TCPSourceGenerator(final int parallelism) {

    this.channels = new Channel[parallelism];
    this.events = new ArrayList<List<EventOrWatermark>>(parallelism);
    for (int i = 0; i < parallelism; i++) {
      events.add(new LinkedList<>());
    }

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
    final List<EventOrWatermark> myEvent = events.get(index);
    synchronized (myEvent) {
      myEvent.add(event);
    }
    // channels[index].writeAndFlush(event);
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

  public class SimpleTCPChannelHandler extends SimpleChannelInboundHandler<Object> {

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      System.out.println("Channel active for readable " + ctx.channel().remoteAddress());
      /*
      synchronized (channels) {
        for (int i = 0; i < channelIndex + 1; i++) {
          if (channels[i] == null) {
            channels[i] = ctx.channel();

            System.out.println("Channel registered for readable " + ctx.channel().remoteAddress() + ", index " + i);

            if (i == channelIndex) {
              channelIndex += 1;
            }
            break;
          }
        }
      }
      */
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object object) throws Exception {
      if (object instanceof TCPInitChannel) {
        final int index = ((TCPInitChannel) object).index;
        channels[index] = ctx.channel();
        System.out.println("Registering channel for index " + index);
      } else if (object instanceof TCPRequstEvent) {
        // send event
        final TCPRequstEvent event = (TCPRequstEvent) object;
        final int index = event.index;
        final List<EventOrWatermark> myEvents = events.get(index);
        synchronized (myEvents) {
          final EventOrWatermark e = myEvents.remove(0);
          ctx.channel().writeAndFlush(new TCPSendEvent(e));
        }
      } else if (object instanceof TCPHasEvent) {
        final TCPHasEvent event = (TCPHasEvent) object;
        final int index = event.index;
        final List<EventOrWatermark> myEvents = events.get(index);

        synchronized (myEvents) {
          final boolean hasEvent = !myEvents.isEmpty() && !(myEvents.size() == 1 && myEvents.get(0).isWatermark());
          // System.out.println("Has event for index " + index + " , " + hasEvent);
          ctx.channel().writeAndFlush(new TCPResponseHasEvent(hasEvent));
        }
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      System.out.println("Channel inactive " + ctx.channel().remoteAddress());
      for (int i = 0; i < channels.length; i++) {
        if (channels[i].equals(ctx.channel())) {
          channels[i] = null;
          System.out.println("Rm channel index " + i);
          break;
        }
      }
      ctx.channel().close();
    }
  }
}
