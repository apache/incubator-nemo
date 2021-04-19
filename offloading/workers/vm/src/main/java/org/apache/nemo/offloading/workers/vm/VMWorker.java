package org.apache.nemo.offloading.workers.vm;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingHandler;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

public class VMWorker {
  private static final Logger LOG = LoggerFactory.getLogger(VMWorker.class.getName());
  private static final int SERVER_BOSS_NUM_THREADS = 3;
  private static final int SERVER_WORKER_NUM_THREADS = 15;
  private static final String CLASS_NAME = VMWorker.class.getName();

  private static final String ADDRESS = "0.0.0.0";

  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private EventLoopGroup serverBossGroup;
  private EventLoopGroup serverWorkerGroup;
  private Channel acceptor;
  private Map<Channel, EventHandler> channelEventHandlerMap;

  private final Map<String, OffloadingHandler.LambdaEventHandler> lambdaEventHandlerMap;

  private final ExecutorService singleThread = Executors.newSingleThreadExecutor();
  private final CountDownLatch countDownLatch = new CountDownLatch(1);
  private final BlockingQueue<OffloadingHandler> handlerQueue = new LinkedBlockingQueue<>();
  private final BlockingQueue<OffloadingMasterEvent> requestQueue = new LinkedBlockingQueue<>();

  private final int timeout;
  private VMWorker(final int port, final int timeout) {
    this.timeout = timeout;

    LOG.info("VM worker port: {}, timeout: {}", port, timeout);

    this.lambdaEventHandlerMap = new ConcurrentHashMap<>();

    serverBossGroup = new NioEventLoopGroup(SERVER_BOSS_NUM_THREADS,
      new DefaultThreadFactory(CLASS_NAME + "SourceServerBoss"));
    serverWorkerGroup = new NioEventLoopGroup(SERVER_WORKER_NUM_THREADS,
      new DefaultThreadFactory(CLASS_NAME + "SourceServerWorker"));

    final ServerBootstrap serverBootstrap = new ServerBootstrap();
    serverBootstrap.group(serverBossGroup, serverWorkerGroup)
      .channel(NioServerSocketChannel.class)
      .childHandler(new NettyChannelInitializer(
        new NettyServerSideChannelHandler(serverChannelGroup, requestQueue)))
      .option(ChannelOption.SO_BACKLOG, 128)
      .option(ChannelOption.SO_REUSEADDR, true)
      .childOption(ChannelOption.SO_KEEPALIVE, true);
    try {
      acceptor = serverBootstrap.bind(
        new InetSocketAddress(ADDRESS, port)).sync().channel();

      LOG.info("Server channel binded " + port);
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private boolean finished = false;
  // blocking
  public void start() throws TimeoutException, ExecutionException {
    while (!finished) {
      try {
        final OffloadingMasterEvent event = requestQueue.take();
        final OffloadingHandler handler;
        if (handlerQueue.isEmpty()) {
          handler =
            new OffloadingHandler(lambdaEventHandlerMap, false, 1000000, true);
        } else {
          handler = handlerQueue.take();
        }

        switch (event.getType()) {
          case SEND_ADDRESS: {
            final byte[] bytes = new byte[event.getByteBuf().readableBytes()];
            event.getByteBuf().readBytes(bytes);

            final String str = new String(bytes);
            System.out.println("Receive request " + str);
            final JSONObject jsonObj = new JSONObject(str);
            final Map<String, Object> map = VMWorkerUtils.jsonToMap(jsonObj);
            final Future future = singleThread.submit(() -> {
              handler.handleRequest(map, null);
              handlerQueue.add(handler);
            });

            try {
              future.get(timeout, TimeUnit.SECONDS);
            } catch (final InterruptedException|TimeoutException e) {
              LOG.info("Handler interrupted due to timeout");
            }
            break;
          }
          default:
            throw new RuntimeException("unsupported type: " + event.getType());
        }

      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    serverWorkerGroup.shutdownGracefully();
    serverBossGroup.shutdownGracefully();
  }


  public static void main(final String[] args) throws InterruptedException, TimeoutException, ExecutionException {

    if (args.length > 0) {
      final int port = Integer.valueOf(args[0]);
      final int timeout = Integer.valueOf(args[1]);
      LOG.info("Start worker with args {}, {}", args[0], args[1]);
      final VMWorker worker = new VMWorker(port, timeout);
      worker.start();
    } else {
      final VMWorker worker = new VMWorker(Constants.VM_WORKER_PORT, 100000);
      worker.start();
    }
    LOG.info("End of worker");
    System.exit(0);
  }

  @ChannelHandler.Sharable
  final class NettyServerSideChannelHandler extends ChannelInboundHandlerAdapter {
    private final Logger LOG = LoggerFactory.getLogger(NettyServerSideChannelHandler.class.getName());
    private final ChannelGroup channelGroup;
    private final BlockingQueue<OffloadingMasterEvent> requestQueue;

    NettyServerSideChannelHandler(final ChannelGroup channelGroup,
                                  final BlockingQueue<OffloadingMasterEvent> requestQueue) {
      this.channelGroup = channelGroup;
      this.requestQueue = requestQueue;
    }

    /**
     * Add the active channel to channelGroup.
     * @param ctx the context object
     * @throws Exception
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
      LOG.info("Channel activate: {}", ctx.channel());
      channelGroup.add(ctx.channel());
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      LOG.info("Channel read from {}, {}", ctx.channel(), msg);
      requestQueue.add((OffloadingMasterEvent)msg);
    }

    /**
     * Remove the inactive channel from channelGroup.
     * @param ctx the context object
     * @throws Exception
     */
    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
      channelGroup.remove(ctx);
      ctx.close();
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
      cause.printStackTrace();
      channelGroup.remove(ctx);
      ctx.close();
    }
  }
}
