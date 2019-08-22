package org.apache.nemo.runtime.lambda;


import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.nemo.runtime.master.resource.LambdaEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LambdaInboundHandler is executed when init channel.
 */
@ChannelHandler.Sharable
public class LambdaExecutorInboundHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaExecutorInboundHandler.class.getName());
  private Channel channel;
  private LambdaExecutor.LambdaEventHandler lambdaEventHandler;

  public void setEventHandler(LambdaExecutor.LambdaEventHandler lambdaEventHandler) {
    this.lambdaEventHandler = lambdaEventHandler;
  }

  /**
   * Add the active channel to channelGroup.
   * @param ctx the context object
   * @throws Exception
   */
  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    System.out.println("Channel activate: " + ctx.channel());
    this.channel = ctx.channel();
  }

  /**
   * Stub function for channelRead.
   * @param ctx
   * @param msg
   * @throws Exception
   */
  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    System.out.println("Channel read from " + ctx.channel() + msg);
    lambdaEventHandler.setChannel(ctx.channel());
    lambdaEventHandler.onNext((LambdaEvent) msg);
  }

  /**
   * Remove the inactive channel from channelGroup.
   * @param ctx the context object
   * @throws Exception
   */
  @Override
  public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
    ctx.close();
  }

  /**
   * Remove ctx when exception caught.
   * @param ctx
   * @param cause
   * @throws Exception
   */
  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
    cause.printStackTrace();
    ctx.close();
  }
}

