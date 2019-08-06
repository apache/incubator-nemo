package org.apache.nemo.runtime.lambdaexecutor.datatransfer;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import org.apache.nemo.common.RendevousMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RendevousClientChannelInitializer extends ChannelInitializer<SocketChannel> {
  private static final Logger LOG = LoggerFactory.getLogger(RendevousClientChannelInitializer.class);

  private RendevousServerClient client;

  public RendevousClientChannelInitializer() {
  }

  public void setRendevousClient(final RendevousServerClient c) {
    client = c;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    LOG.info("Registering rendevous channel {}", ch);

    ch.pipeline()
      // outbound
      .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
        Integer.MAX_VALUE, 0, 4, 0, 4))
      .addLast("frameEncoder", new LengthFieldPrepender(4))
      .addLast(new RendevousMessageEncoder())

      // inbound
      .addLast(new RendevousClientDecoder(client));
  }
}
