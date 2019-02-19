package org.apache.nemo.offloading.common;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.ConcurrentMap;


@ChannelHandler.Sharable
public final class NettyLambdaInboundHandler extends ChannelInboundHandlerAdapter {

  private final ConcurrentMap<Channel, EventHandler<OffloadingEvent>> channelMap;

  public NettyLambdaInboundHandler(final ConcurrentMap<Channel, EventHandler<OffloadingEvent>> channelMap) {
    this.channelMap = channelMap;
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
      final EventHandler<OffloadingEvent> eventHandler = channelMap.get(ctx.channel());
      if (eventHandler != null) {
        eventHandler.onNext((OffloadingEvent) msg);
        return;
      }

      System.out.println("No event handler for msg " + msg.toString());

      Thread.sleep(100);
    }

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
