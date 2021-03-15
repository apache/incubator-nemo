package org.apache.nemo.offloading.common;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class initializes socket channel for text messages.
 */
public final class NettyChannelInitializer
    extends ChannelInitializer<SocketChannel> {

  private static final Logger LOG = LoggerFactory.getLogger(NettyChannelInitializer.class.getName());

  private final ChannelInboundHandlerAdapter inboundHandlerAdapter;

  public NettyChannelInitializer(final ChannelInboundHandlerAdapter inboundHandlerAdapter) {
    this.inboundHandlerAdapter = inboundHandlerAdapter;
  }

  /**
   * Initializes the socket channel with lineBasedFrame decoder for text messages.
   * @param ch
   * @throws Exception
   */
  @Override
  protected void initChannel(final SocketChannel ch) throws Exception {

    LOG.info("Init channel {}", ch);

    ch.pipeline()
      .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
        1024 * 1024 * 25, 0, 4, 0, 4))
      .addLast("frameEncoder", new LengthFieldPrepender(4))
      .addLast("decoder", new OffloadingEventCoder.OffloadingEventDecoder())
      .addLast("encoder", new OffloadingEventCoder.OffloadingEventEncoder())
      .addLast("handler", inboundHandlerAdapter);
  }

  /**
   * Remove the inactive channel from channelGroup.
   * @param ctx the context object
   * @throws Exception
  @Override
  public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
    LOG.info("Channel inactive {}", ctx.channel());
    inboundHandlerAdapter.channelInactive(ctx);
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    LOG.info("Channel activated {}", ctx.channel());
    inboundHandlerAdapter.channelActive(ctx);
  }
   */
}
