package org.apache.nemo.runtime.executor.task.util;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.punctuation.TimestampAndValue;

public final class TCPSourceReadable implements Readable {

  List<EventOrWatermark> elements;
  private long currWatermark = 0;

  @Override
  public void prepare() {
    elements = new LinkedList<>();
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();

    // Help boot strapping a channel
    Bootstrap clientBootstrap = new Bootstrap();
    clientBootstrap
      .group(eventLoopGroup) // associate event loop to channel
      .channel(NioSocketChannel.class) // create a NIO socket channel
      .handler(new TCPClientChannelInitializer(elements)); // Add channel initializer

    try {
      // Connect to listening server
      ChannelFuture channelFuture = clientBootstrap.connect("localhost", TCPSourceGenerator.PORT).sync();
      // Check if channel is connected
      if(!channelFuture.isSuccess()) {
        throw new RuntimeException("server not connected");
      }
      // Block till channel is connected
      // channelFuture.channel().closeFuture().sync();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      // Connection is closed, clean up
      // eventLoopGroup.shutdownGracefully();
    }
  }

  @Override
  public boolean isAvailable() {
    synchronized (elements) {
      return !elements.isEmpty() && !(elements.size() == 1 && elements.get(0).isWatermark());
    }
  }

  @Override
  public Object readCurrent() throws NoSuchElementException {
    while (true) {
      if (elements.isEmpty()) {
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } else {
        final EventOrWatermark e = elements.remove(0);
        if (e.isWatermark()) {
          currWatermark = e.watermark;
        } else {
          return new TimestampAndValue<>(System.currentTimeMillis(), e.event);
        }
      }
    }
  }

  @Override
  public long readWatermark() {
    return currWatermark;
  }

  @Override
  public boolean isFinished() {
    return false;
  }

  @Override
  public List<String> getLocations() throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
  }

  class TCPClientChannelInitializer extends ChannelInitializer<SocketChannel > {

    private final List<EventOrWatermark> eventOrWatermarks;

    public TCPClientChannelInitializer(final List<EventOrWatermark> eventOrWatermarks) {
      this.eventOrWatermarks = eventOrWatermarks;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) {
      // Configure encoders/decoder or codecs
      socketChannel.pipeline().addLast(new ObjectDecoder(ClassResolvers
        .cacheDisabled(getClass().getClassLoader())));// (2)
      socketChannel.pipeline().addLast(new ObjectEncoder());
      // Add Custom Inbound handler to handle incoming traffic
      socketChannel.pipeline().addLast(new TCPClientInboundHandler(eventOrWatermarks));
    }
  }

  class TCPClientInboundHandler extends SimpleChannelInboundHandler<EventOrWatermark> {

    private final List<EventOrWatermark> events;

    public TCPClientInboundHandler(final List<EventOrWatermark> events) {
      this.events = events;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, EventOrWatermark s) {
      if (s.isWatermark()) {
        System.out.println("Watermark recekved!!" + s.watermark);
      }
      synchronized (events) {
        events.add(s);
      }
    }
  }
}
