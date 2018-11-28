package org.apache.nemo.runtime.lambda;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.NemoEvent;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;


public final class NettyLambdaInboundHandler extends ChannelInboundHandlerAdapter {

  private final ConcurrentMap<Channel, EventHandler<NemoEvent>> channelMap;

  public NettyLambdaInboundHandler(final ConcurrentMap<Channel, EventHandler<NemoEvent>> channelMap) {
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
      final EventHandler<NemoEvent> eventHandler = channelMap.get(ctx.channel());
      if (eventHandler != null) {
        eventHandler.onNext((NemoEvent) msg);
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
    ctx.close();
    //channelMap.remove(ctx.channel());
  }
}
