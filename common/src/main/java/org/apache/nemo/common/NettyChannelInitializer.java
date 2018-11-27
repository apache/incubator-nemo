package org.apache.nemo.common;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

/**
 * This class initializes socket channel for text messages.
 */
public final class NettyChannelInitializer
    extends ChannelInitializer<SocketChannel> {

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
    ch.pipeline()
      .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
        Integer.MAX_VALUE, 0, 4, 0, 4))
      .addLast("frameEncoder", new LengthFieldPrepender(4))
      .addLast("decoder", new NemoEventCoder.NemoEventDecoder())
      .addLast("encoder", new NemoEventCoder.NemoEventEncoder())
      .addLast("handler", inboundHandlerAdapter);
  }
}
