package org.apache.nemo.offloading.common;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@ChannelHandler.Sharable
public final class NettyLambdaInboundHandler extends ChannelInboundHandlerAdapter {
  private final ConcurrentMap<Channel, EventHandler<OffloadingMasterEvent>> channelMap;
  private final ExecutorService executorService;

  public NettyLambdaInboundHandler(final ConcurrentMap<Channel, EventHandler<OffloadingMasterEvent>> channelMap) {
    this.channelMap = channelMap;
    this.executorService = Executors.newCachedThreadPool();
  }

  /**
   * Forward the msg to the handler.
   * @param ctx the context object.
   * @param msg the message.
   * @throws Exception
   */
  @Override
  public void channelRead(
    final ChannelHandlerContext ctx, final Object msg) throws Exception {
    while (true) {
      final EventHandler<OffloadingMasterEvent> eventHandler = channelMap.get(ctx.channel());
      if (eventHandler != null) {
        executorService.execute(() -> {
          eventHandler.onNext((OffloadingMasterEvent) msg);
        });
        return;
      }

      System.out.println("No event handler for msg " + msg.toString());

      Thread.sleep(100);
    }

  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    System.out.print("Channel inactive... " + ctx.channel());
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
    cause.printStackTrace();
    System.out.println("Exception1!: " + cause.toString());
    channelMap.remove(ctx.channel());
    ctx.close();
    //channelMap.remove(ctx.channel());
  }
}
