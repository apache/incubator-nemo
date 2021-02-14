package org.apache.nemo.runtime.lambdaexecutor;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.runtime.executor.common.controlmessages.state.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public final class NettyVMStateStoreClient implements StateStore {


  private final EventLoopGroup eventLoopGroup;
  private final Channel channel;
  private final Map<String, CountDownLatch> latchMap;
  private final Map<String, Object> responseMap;

  public NettyVMStateStoreClient(final String address,
                                 final int port) {

    this.eventLoopGroup = new NioEventLoopGroup();
    this.latchMap = new ConcurrentHashMap<>();
    this.responseMap = new ConcurrentHashMap<>();

    // Help boot strapping a channel
    Bootstrap clientBootstrap = new Bootstrap();
    clientBootstrap
      .group(eventLoopGroup) // associate event loop to channel
      .channel(NioSocketChannel.class) // create a NIO socket channel
      .handler(new VMStatestoreClientChannelInitializer()); // Add channel initializer

    try {
      // Connect to listening server
      ChannelFuture channelFuture = clientBootstrap.connect(address, port).sync();
      channel = channelFuture.channel();
      // Check if channel is connected
      if(!channelFuture.isSuccess()) {
        throw new RuntimeException("server not connected");
      }

      System.out.println("VM State store client connected");
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
  public InputStream getStateStream(String taskId) {
    final String key = "get-" + taskId;
    latchMap.put(key, new CountDownLatch(1));
    channel.writeAndFlush(new GetState(taskId));
    try {
      latchMap.get(key).await();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    latchMap.remove(key);
    final GetStateResponse response = (GetStateResponse) responseMap.remove(key);
    return new ByteArrayInputStream(response.bytes);
  }

  @Override
  public byte[] getBytes(String taskId) {
    final String key = "get-" + taskId;
    latchMap.put(key, new CountDownLatch(1));
    channel.writeAndFlush(new GetState(taskId));
    try {
      latchMap.get(key).await();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    latchMap.remove(key);
    final GetStateResponse response = (GetStateResponse) responseMap.remove(key);
    return response.bytes;
  }

  @Override
  public OutputStream getOutputStream(String taskId) {
    final ByteBufOutputStream bos = new ByteBufOutputStream(channel.alloc().ioBuffer());
    return new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        bos.write(b);
      }

      @Override
      public void close() {
        try {
          bos.close();
          final String key = "put-" + taskId;
          latchMap.put(key, new CountDownLatch(1));
          channel.writeAndFlush(new PutState(taskId,
            bos.buffer().array()));

          try {
            latchMap.get(key).await();
          } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }

          latchMap.remove(key);
          responseMap.remove(key);
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    };
  }

  @Override
  public void put(String taskId, byte[] bytes) {
    final String key = "put-" + taskId;
    latchMap.put(key, new CountDownLatch(1));
    channel.writeAndFlush(new PutState(taskId, bytes));

    try {
      latchMap.get(key).await();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    latchMap.remove(key);
    responseMap.remove(key);
  }

  @Override
  public boolean containsState(String taskId) {
    final String key = "contains-" + taskId;
    latchMap.put(key, new CountDownLatch(1));
    channel.writeAndFlush(new ContainsState(taskId));
    try {
      latchMap.get(key).await();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    latchMap.remove(key);
    final ContainsStateResponse response = (ContainsStateResponse) responseMap.remove(key);
    return response.result;
  }

  @Override
  public void close() {
    channel.close();
    eventLoopGroup.shutdownGracefully();
  }

  final class VMStatestoreClientChannelInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      ch.pipeline()
      .addLast(new ObjectEncoder()) // (1)
      .addLast(new ObjectDecoder(ClassResolvers
        .cacheDisabled(getClass().getClassLoader())))// (2)
      .addLast(new NettyVmStateStoreClientHandler()); // (3)
    }
  }

  final class NettyVmStateStoreClientHandler extends SimpleChannelInboundHandler<Object> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof ContainsStateResponse) {
        final ContainsStateResponse cs = (ContainsStateResponse) msg;
        responseMap.put("contains-"  + cs.taskId, cs);
        latchMap.get("contains-" + cs.taskId).countDown();
      } else if (msg instanceof GetStateResponse) {
        final GetStateResponse cs = (GetStateResponse) msg;
        responseMap.put("get-"  + cs.taskId, cs);
        latchMap.get("get-" + cs.taskId).countDown();
      } else if (msg instanceof PutStateResponse) {
        final PutStateResponse cs = (PutStateResponse) msg;
        responseMap.put("put-"  + cs.taskId, cs);
        latchMap.get("put-" + cs.taskId).countDown();
      }
    }
  }
}
