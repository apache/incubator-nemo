package org.apache.nemo.runtime.message.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.message.MessageSender;
import org.apache.nemo.runtime.message.ReplyFutureMap;
import org.apache.nemo.runtime.message.ncs.NcsMessageSender;
import org.apache.reef.io.network.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class NettyMessageSender implements MessageSender<ControlMessage.Message> {
  private static final Logger LOG = LoggerFactory.getLogger(NettyMessageSender.class.getName());

  private final Channel channel;
  private final ReplyFutureMap<ControlMessage.Message> replyFutureMap;

  public NettyMessageSender(
    final Channel channel,
    final ReplyFutureMap replyFutureMap) {
    this.channel = channel;
    this.replyFutureMap = replyFutureMap;
  }

  @Override
  public void send(final ControlMessage.Message message) {
    // LOG.info("Write ncs message {}", message);
    final ByteBuf byteBuf = channel.alloc().ioBuffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);
    try {
      message.writeTo(bos);
      bos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    channel.writeAndFlush(byteBuf);
  }

  @Override
  public CompletableFuture<ControlMessage.Message> request(final ControlMessage.Message message) {
    // LOG.info("Request ncs message {}", message);
    final CompletableFuture<ControlMessage.Message> future = replyFutureMap.beforeRequest(message.getId());
    final ByteBuf byteBuf = channel.alloc().ioBuffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);
    try {
      message.writeTo(bos);
      bos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    channel.writeAndFlush(byteBuf);
    return future;
  }

  @Override
  public void close() throws Exception {

  }
}
