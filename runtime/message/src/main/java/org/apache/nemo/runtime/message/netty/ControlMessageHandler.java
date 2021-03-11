package org.apache.nemo.runtime.message.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.nemo.offloading.common.Pair;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.message.MessageEnvironment;
import org.apache.nemo.runtime.message.MessageListener;
import org.apache.nemo.runtime.message.ReplyFutureMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.apache.nemo.runtime.common.comm.ControlMessage.MessageType.InitRegistration;
import static org.apache.nemo.runtime.message.netty.ControlMessageWrapper.MsgType.Reply;

@ChannelHandler.Sharable
public final class ControlMessageHandler extends SimpleChannelInboundHandler<ControlMessageWrapper> {
  private static final Logger LOG = LoggerFactory.getLogger(ControlMessageHandler.class.getName());

  private final Map<String, Channel> channelMap;
  private final Map<MessageEnvironment.ListenerType, MessageListener> handlerMap;
  private final ReplyFutureMap<ControlMessage.Message> replyFutureMap;
  private final String myId;
  private final ExecutorService replyService;

  public ControlMessageHandler(
    final String myId,
    final Map<String, Channel> channelMap,
    final Map<MessageEnvironment.ListenerType, MessageListener> handlerMap,
    final ReplyFutureMap<ControlMessage.Message> replyFutureMap,
    final ExecutorService replyService) {
    this.myId = myId;
    this.channelMap = channelMap;
    this.handlerMap = handlerMap;
    this.replyFutureMap = replyFutureMap;
    this.replyService = replyService;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    throws Exception {
    cause.printStackTrace();
    throw new RuntimeException("Exception caused " + cause);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ControlMessageWrapper message) throws Exception {
    final ControlMessage.Message msg = message.msg;

    // LOG.info("Control message handler in " + myId);

    if (msg.getType().equals(InitRegistration)) {
      final String senderId = msg.getSenderId();
      LOG.info("Init registration for {}", senderId);
      channelMap.putIfAbsent(senderId, ctx.channel());
    } else {
      final MessageEnvironment.ListenerType type = MessageEnvironment.ListenerType.values()[msg.getListenerId()];
      replyService.execute(() -> {
        if (handlerMap.containsKey(type)) {
          switch (message.type) {
            case Send: {
              // LOG.info("Receive send message in {} {}/{}", myId, msg.getId(), msg.getType());
              handlerMap.get(type).onMessage(msg);
              break;
            }
            case Request: {
              // LOG.info("Receive request message in {} {}/{}", myId, msg.getId(), msg.getType());
              handlerMap.get(type).onMessageWithContext(msg,
                new NettyMessageContext(myId, msg.getId(), ctx.channel()));
              break;
            }
            case Reply: {
              // LOG.info("Receive reply message in {} {}/{}", myId, msg.getId(), msg.getType());
              replyFutureMap.onSuccessMessage(msg.getId(), msg);
              break;
            }
            default:
              throw new RuntimeException("Invalid type " + type + ", message" + message);
          }
        } else {
          if (message.type.equals(Reply)) {
            // LOG.info("Receive reply message in {} hoho {}/{}", myId, msg.getId(), msg.getType());
            replyFutureMap.onSuccessMessage(msg.getId(), msg);
          } else {
            LOG.warn("Not registered listener type " + type);
            throw new RuntimeException("Not registered listener type " + type);
          }
        }
      });
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    LOG.warn("Channel inactivated {}", ctx.channel());
  }
}
