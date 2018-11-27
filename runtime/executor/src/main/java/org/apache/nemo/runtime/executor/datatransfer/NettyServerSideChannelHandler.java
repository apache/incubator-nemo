package org.apache.nemo.runtime.executor.datatransfer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.NemoEvent;


final class NettyServerSideChannelHandler extends ChannelInboundHandlerAdapter {

  private final ChannelGroup channelGroup;
  private final EventHandler<NemoEvent> eventHandler;

  NettyServerSideChannelHandler(final ChannelGroup channelGroup,
                                final EventHandler<NemoEvent> eventHandler) {
    this.channelGroup = channelGroup;
    this.eventHandler = eventHandler;
  }

  /**
   * Add the active channel to channelGroup.
   * @param ctx the context object
   * @throws Exception
   */
  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    channelGroup.add(ctx.channel());
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    eventHandler.onNext((NemoEvent)msg);
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
