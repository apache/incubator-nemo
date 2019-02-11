package org.apache.nemo.offloading.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ChannelHandler.Sharable
public final class NettyServerSideChannelHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(NettyServerSideChannelHandler.class.getName());
  private final ChannelGroup channelGroup;
  private final EventHandler<Pair<Channel,NemoEvent>> eventHandler;

  public NettyServerSideChannelHandler(final ChannelGroup channelGroup,
                                       final EventHandler<Pair<Channel,NemoEvent>> eventHandler) {
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
    LOG.info("Channel activate: {}", ctx.channel());
    channelGroup.add(ctx.channel());
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    LOG.info("Channel read from {}, {}", ctx.channel(), msg);
    eventHandler.onNext(Pair.of(ctx.channel(), (NemoEvent)msg));
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
