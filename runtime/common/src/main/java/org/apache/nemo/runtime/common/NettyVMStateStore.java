package org.apache.nemo.runtime.common;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.common.NettyServerTransport;
import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.common.nettyvm.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.ports.TcpPortProvider;

import javax.inject.Inject;
import java.io.InputStream;
import java.io.OutputStream;

public final class NettyVMStateStore implements StateStore {

  private final String publicAddress;
  private int bindingPort;
  private final NettyServerTransport nettyServerTransport;
  private final HDFStateStore stateStore;

  @Inject
  private NettyVMStateStore(final TcpPortProvider tcpPortProvider,
                            final HDFStateStore stateStore,
                            @Parameter(EvalConf.Ec2.class) final boolean ec2) {
    this.nettyServerTransport = new NettyServerTransport(tcpPortProvider,
      new VMStatestoreChannelInitializer(),
      new NioEventLoopGroup(3,
      new DefaultThreadFactory("VMStateStore")),
      false);
    this.publicAddress = nettyServerTransport.getPublicAddress();
    this.bindingPort = nettyServerTransport.getPort();
    this.stateStore = stateStore;

  }

  @Override
  public int getPort() {
    return bindingPort;
  }

  @Override
  public InputStream getStateStream(String taskId) {
    return stateStore.getStateStream(taskId);
  }

  @Override
  public byte[] getBytes(String taskId) {
    return stateStore.getBytes(taskId);
  }

  @Override
  public OutputStream getOutputStream(String taskId) {
    return stateStore.getOutputStream(taskId);
  }

  @Override
  public void put(String taskId, byte[] bytes) {
    stateStore.put(taskId, bytes);
  }

  @Override
  public boolean containsState(String taskId) {
    return stateStore.containsState(taskId);
  }

  @Override
  public void close() {
      nettyServerTransport.close();
  }

  final class VMStatestoreChannelInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      ch.pipeline()
      .addLast(new ObjectEncoder()) // (1)
      .addLast(new ObjectDecoder(ClassResolvers
        .cacheDisabled(getClass().getClassLoader())))// (2)
      .addLast(new NettyVmStateStoreHandler()); // (3)
    }
  }

  final class NettyVmStateStoreHandler extends SimpleChannelInboundHandler<Object> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof ContainsState) {
        final ContainsState cs = (ContainsState) msg;
        ctx.channel().writeAndFlush(
          new ContainsStateResponse(cs.taskId, stateStore.containsState(cs.taskId)));
      } else if (msg instanceof GetState) {
        final GetState cs = (GetState) msg;
        ctx.channel().writeAndFlush(
          new GetStateResponse(cs.taskId, stateStore.getBytes(cs.taskId)));
      } else if (msg instanceof PutState) {
        final PutState m = (PutState) msg;
        stateStore.put(m.taskId, m.bytes);
        ctx.channel().writeAndFlush(
          new PutStateResponse(m.taskId));
      }
    }
  }
}
