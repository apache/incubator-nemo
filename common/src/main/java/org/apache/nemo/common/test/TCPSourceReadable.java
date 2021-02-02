package org.apache.nemo.common.test;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TCPSourceReadable implements Readable {
  private static final Logger LOG = LoggerFactory.getLogger(TCPSourceReadable.class.getName());
  public static final int PORT = 12512;

  // Queue<EventOrWatermark> elements;
  // Queue<EventOrWatermark> watermarks;
  private long currWatermark = 0;
  private ReadableContext readableContext;
  private EventLoopGroup eventLoopGroup;
  private Channel channel;

  public final int index;

  private CountDownLatch hasEventLatch;
  private volatile boolean hasEvent;

  private CountDownLatch currEventLatch;
  private volatile EventOrWatermark currEvent;

  public TCPSourceReadable(final int index) {
    this.index = index;
  }

  @Override
  public void prepare(final ReadableContext readableContext) {
    // elements = new ConcurrentLinkedQueue<>();
    // watermarks = new ConcurrentLinkedQueue<>();
    eventLoopGroup = new NioEventLoopGroup();
    this.readableContext = readableContext;

    // Help boot strapping a channel
    Bootstrap clientBootstrap = new Bootstrap();
    clientBootstrap
      .group(eventLoopGroup) // associate event loop to channel
      .channel(NioSocketChannel.class) // create a NIO socket channel
      .handler(new TCPClientChannelInitializer()); // Add channel initializer

    try {
      // Connect to listening server
      ChannelFuture channelFuture = clientBootstrap.connect("localhost", PORT).sync();
      channel = channelFuture.channel();
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
//    if (index == 2) {
//      LOG.info("Available start for index {}", 2);
//    }
    hasEventLatch = new CountDownLatch(1);
    channel.writeAndFlush(new TCPHasEvent(index));

    try {
      hasEventLatch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
//     if (index == 2) {
//      LOG.info("Available end for index {}, isAvailable {}", 2, hasEvent);
//    }

    return hasEvent;
  }

  @Override
  public Object readCurrent() throws NoSuchElementException {

    while (true) {
      currEventLatch = new CountDownLatch(1);
      channel.writeAndFlush(new TCPRequstEvent(index));

      try {
        currEventLatch.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      if (currEvent.isWatermark()) {
        currWatermark = currEvent.watermark;
      } else {
        return new TimestampAndValue<>(System.currentTimeMillis(), currEvent.event);
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
    System.out.println("Closing TCPSourceReader in " + readableContext.getTaskId());
    channel.close();
    eventLoopGroup.shutdownGracefully();
  }

  class TCPClientChannelInitializer extends ChannelInitializer<SocketChannel > {

    public TCPClientChannelInitializer() {
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) {
      // Configure encoders/decoder or codecs
      socketChannel.pipeline().addLast(new ObjectDecoder(ClassResolvers
        .cacheDisabled(getClass().getClassLoader())));// (2)
      socketChannel.pipeline().addLast(new ObjectEncoder());
      // Add Custom Inbound handler to handle incoming traffic
      socketChannel.pipeline().addLast(new TCPClientInboundHandler());
    }
  }

  class TCPClientInboundHandler extends SimpleChannelInboundHandler<Object> {


    public TCPClientInboundHandler() {
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      // send my index
      System.out.println("TCP Init Channel index send " + index);
      ctx.writeAndFlush(new TCPInitChannel(index));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object s) {
      if (s instanceof TCPSendEvent) {
        final EventOrWatermark event = ((TCPSendEvent) s).event;
        currEvent = event;
        currEventLatch.countDown();
      } else if (s instanceof TCPResponseHasEvent) {
        hasEvent = ((TCPResponseHasEvent) s).hasEvent;
        hasEventLatch.countDown();
      }
    }
  }
}
