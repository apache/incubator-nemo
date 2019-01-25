package org.apache.nemo.runtime.vm;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.NettyChannelInitializer;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.lambda.Constants;
import org.apache.nemo.common.lambda.OffloadingHandler;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

public class VMWorker {
  private static final Logger LOG = LoggerFactory.getLogger(VMWorker.class.getName());
  private static final int SERVER_BOSS_NUM_THREADS = 3;
  private static final int SERVER_WORKER_NUM_THREADS = 10;
  private static final String CLASS_NAME = VMWorker.class.getName();

  private static final String ADDRESS = "172.31.37.143";

  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private EventLoopGroup serverBossGroup;
  private EventLoopGroup serverWorkerGroup;
  private Channel acceptor;
  private Map<Channel, EventHandler> channelEventHandlerMap;

    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final ExecutorService singleThread = Executors.newSingleThreadExecutor();
  private VMWorker() {
      serverBossGroup = new NioEventLoopGroup(SERVER_BOSS_NUM_THREADS,
        new DefaultThreadFactory(CLASS_NAME + "SourceServerBoss"));
      serverWorkerGroup = new NioEventLoopGroup(SERVER_WORKER_NUM_THREADS,
        new DefaultThreadFactory(CLASS_NAME + "SourceServerWorker"));

      final OffloadingHandler handler = new OffloadingHandler(() -> {
        return this.getClass().getClassLoader();
      });

    final BlockingQueue<NemoEvent> requestQueue = new LinkedBlockingQueue<>();
    singleThread.execute(() -> {
      while (true) {
        try {
          final NemoEvent event = requestQueue.take();
          executorService.execute(() -> {
            final String str = new String(event.getBytes());
            System.out.println("Receive request " + str);
            final JSONObject jsonObj = new JSONObject(str);
            final Map<String, Object> map = VMWorkerUtils.jsonToMap(jsonObj);
            handler.handleRequest(map);
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
    private final BlockingQueue<NemoEvent> requestQueue;

    NettyServerSideChannelHandler(final ChannelGroup channelGroup,
                                  final BlockingQueue<NemoEvent> requestQueue) {
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
      requestQueue.add((NemoEvent)msg);
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
