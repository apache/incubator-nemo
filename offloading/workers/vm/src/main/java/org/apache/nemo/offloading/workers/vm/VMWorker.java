package org.apache.nemo.offloading.workers.vm;

import io.netty.bootstrap.Bootstrap;
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
import org.apache.nemo.offloading.common.OffloadingHandler;

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

  private final Map<String, OffloadingHandler.LambdaEventHandler> lambdaEventHandlerMap;

  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private final ExecutorService singleThread = Executors.newSingleThreadExecutor();


  private String nameServerAddr;
  private int nameServerPort = 0;
  private String newExecutorId;

  private OffloadingHandler offloadingHandler;

  private VMWorker() {

    this.lambdaEventHandlerMap = new ConcurrentHashMap<>();

      serverBossGroup = new NioEventLoopGroup(SERVER_BOSS_NUM_THREADS,
        new DefaultThreadFactory(CLASS_NAME + "SourceServerBoss"));
      serverWorkerGroup = new NioEventLoopGroup(SERVER_WORKER_NUM_THREADS,
        new DefaultThreadFactory(CLASS_NAME + "SourceServerWorker"));

      final BlockingQueue<OffloadingHandler> handlers = new LinkedBlockingQueue<>();

    final BlockingQueue<OffloadingEvent> requestQueue = new LinkedBlockingQueue<>();
    singleThread.execute(() -> {
      while (true) {
        try {
          final OffloadingEvent event = requestQueue.take();
          executorService.execute(() -> {
            switch (event.getType()) {
              case CLIENT_HANDSHAKE: {
                final OffloadingHandler handler = new OffloadingHandler(
                  lambdaEventHandlerMap, false);
                offloadingHandler = handler;
                handlers.add(handler);
                final byte[] bytes = new byte[event.getByteBuf().readableBytes()];
                event.getByteBuf().readBytes(bytes);

                final String str = new String(bytes);
                System.out.println("Receive request " + str);
                final JSONObject jsonObj = new JSONObject(str);
                final Map<String, Object> map = VMWorkerUtils.jsonToMap(jsonObj);
                handler.handleRequest(map);
                break;
              }
              case DATA: {
                // It receives data from upstream tasks
                // We first retrieve taskId
                final ByteBuf byteBuf = event.getByteBuf();
                final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
                final DataInputStream dataInputStream = new DataInputStream(bis);
                try {
                  final String taskId = dataInputStream.readUTF();
                  LOG.info("Receive data for task {}", taskId);

                  lambdaEventHandlerMap.get(taskId).onNext(event);
                } catch (IOException e) {
                  e.printStackTrace();
                  throw new RuntimeException(e);
                }
                break;
              }
              default:
                throw new RuntimeException("unsupported type: " + event.getType());
            }

          });
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });


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
        new InetSocketAddress(ADDRESS, Constants.VM_WORKER_PORT)).sync().channel();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }


  public static void main(final String[] args) throws InterruptedException {
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    LOG.info("Start worker");
    final VMWorker worker = new VMWorker();
    LOG.info("Wait for request");
    countDownLatch.await();
  }


  @ChannelHandler.Sharable
  final class NettyServerSideChannelHandler extends ChannelInboundHandlerAdapter {
    private final Logger LOG = LoggerFactory.getLogger(NettyServerSideChannelHandler.class.getName());
    private final ChannelGroup channelGroup;
    private final BlockingQueue<OffloadingEvent> requestQueue;

    NettyServerSideChannelHandler(final ChannelGroup channelGroup,
                                  final BlockingQueue<OffloadingEvent> requestQueue) {
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
      requestQueue.add((OffloadingEvent)msg);
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
