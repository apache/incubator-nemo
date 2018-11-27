package org.apache.nemo.common;

import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Factory that creates a Netty channel handler.
 */
public interface NettyChannelHandlerFactory {

  /**
   * Creates a channel inbound handler.
   * @return a channel inbound handler adapter
   */
  ChannelInboundHandlerAdapter createChannelInboundHandler();
}
