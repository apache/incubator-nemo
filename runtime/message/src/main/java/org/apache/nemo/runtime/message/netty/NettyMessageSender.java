package org.apache.nemo.runtime.message.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.nemo.runtime.message.comm.ControlMessage;
import org.apache.nemo.runtime.message.MessageSender;
import org.apache.nemo.runtime.message.ReplyFutureMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class NettyMessageSender implements MessageSender<ControlMessage.Message> {
  private static final Logger LOG = LoggerFactory.getLogger(NettyMessageSender.class.getName());

  private final Channel channel;
  private final ReplyFutureMap<ControlMessage.Message> replyFutureMap;
  private final String senderId;

  public NettyMessageSender(
    final String senderId,
    final Channel channel,
    final ReplyFutureMap replyFutureMap) {
    this.senderId = senderId;
    this.channel = channel;
    this.replyFutureMap = replyFutureMap;
  }

  @Override
  public void send(final ControlMessage.Message message) {
    // LOG.info("Send message from {} through {} {}/{}", senderId,
    //  channel.localAddress(), message.getId(), message.getType());
    // LOG.info("Write ncs message {}", message);
    channel.writeAndFlush(new ControlMessageWrapper(ControlMessageWrapper.MsgType.Send, message));
  }

  @Override
  public CompletableFuture<ControlMessage.Message> request(final ControlMessage.Message message) {
    // LOG.info("Request message from {} through {} {}/{}/{} to {}", senderId,
    //  channel.localAddress(), message.getId(), message.getType(), message.getListenerId(), channel.remoteAddress());

    final CompletableFuture<ControlMessage.Message> future = replyFutureMap.beforeRequest(message.getId());
    channel.writeAndFlush(new ControlMessageWrapper(ControlMessageWrapper.MsgType.Request, message));
    return future;
  }

  @Override
  public void close() throws Exception {

  }
}
