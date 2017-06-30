package edu.snu.vortex.runtime.common.message.ncs;

import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageContext;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageListener;
import edu.snu.vortex.runtime.common.message.MessageSender;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Message environment for NCS.
 * TODO #206: Rethink/Refactor NCS as our RPC stack
 */
public final class NcsMessageEnvironment implements MessageEnvironment {
  private static final Logger LOG = Logger.getLogger(NcsMessageEnvironment.class.getName());

  private static final String NCS_CONN_FACTORY_ID = "NCS_CONN_FACTORY_ID";

  private final NetworkConnectionService networkConnectionService;
  private final IdentifierFactory idFactory;
  private final String senderId;

  private final ReplyWaitingLock replyWaitingLock;
  private final ConcurrentMap<String, MessageListener> listenerConcurrentMap;
  private final ConnectionFactory<ControlMessage.Message> connectionFactory;


  @Inject
  private NcsMessageEnvironment(
      final NetworkConnectionService networkConnectionService,
      final IdentifierFactory idFactory,
      final ReplyWaitingLock replyWaitingLock,
      @Parameter(NcsParameters.SenderId.class) final String senderId) {
    this.networkConnectionService = networkConnectionService;
    this.idFactory = idFactory;
    this.senderId = senderId;
    this.replyWaitingLock = replyWaitingLock;
    this.listenerConcurrentMap = new ConcurrentHashMap<>();
    this.connectionFactory = networkConnectionService.registerConnectionFactory(
        idFactory.getNewInstance(NCS_CONN_FACTORY_ID),
        new ControlMessageCodec(),
        new NcsMessageHandler(),
        new NcsLinkListener(),
        idFactory.getNewInstance(senderId));
  }

  @Override
  public <T> void setupListener(final String listenerId, final MessageListener<T> listener) {
    if (listenerConcurrentMap.putIfAbsent(listenerId, listener) != null) {
      throw new RuntimeException("A listener for " + listenerId + " was already setup");
    }
  }

  @Override
  public <T> Future<MessageSender<T>> asyncConnect(final String receiverId, final String messageTypeId) {
    try {
      final Connection<ControlMessage.Message> connection = connectionFactory.newConnection(
          idFactory.getNewInstance(receiverId));
      connection.open();
      return CompletableFuture.completedFuture((MessageSender) new NcsMessageSender(connection, replyWaitingLock));
    } catch (final Exception e) {
      final CompletableFuture<MessageSender<T>> failedFuture = new CompletableFuture<>();
      failedFuture.completeExceptionally(e);
      return failedFuture;
    }
  }

  @Override
  public String getId() {
    return senderId;
  }

  @Override
  public void close() throws Exception {
    networkConnectionService.close();
  }

  /**
   * Message handler for NCS.
   */
  private final class NcsMessageHandler implements EventHandler<Message<ControlMessage.Message>> {

    public void onNext(final Message<ControlMessage.Message> messages) {
      final ControlMessage.Message controlMessage = extractSingleMessage(messages);
      LOG.log(Level.FINE, "received: {0}", controlMessage);
      final MessageType messageType = getMsgType(controlMessage);
      switch (messageType) {
        case Send:
          processSendMessage(controlMessage);
          break;
        case Request:
          processRequestMessage(controlMessage);
          break;
        case Reply:
          processReplyMessage(controlMessage);
          break;
        default:
          throw new IllegalArgumentException(controlMessage.toString());
      }
    }

    private void processSendMessage(final ControlMessage.Message controlMessage) {
      final String messageType = getListenerId(controlMessage);
      listenerConcurrentMap.get(messageType).onMessage(controlMessage);
    }

    private void processRequestMessage(final ControlMessage.Message controlMessage) {
      final String messageType = getListenerId(controlMessage);
      final String executorId = getExecutorId(controlMessage);
      final MessageContext messageContext = new NcsMessageContext(executorId, connectionFactory, idFactory);
      listenerConcurrentMap.get(messageType).onMessageWithContext(controlMessage, messageContext);
    }

    private void processReplyMessage(final ControlMessage.Message controlMessage) {
      final long requestId = getRequestId(controlMessage);
      replyWaitingLock.onSuccessMessage(requestId, controlMessage);
    }
  }

  /**
   * LinkListener for NCS.
   */
  private final class NcsLinkListener implements LinkListener<Message<ControlMessage.Message>> {

    public void onSuccess(final Message<ControlMessage.Message> messages) {
      // No-ops.
    }

    public void onException(final Throwable throwable,
                            final SocketAddress socketAddress,
                            final Message<ControlMessage.Message> messages) {
      final ControlMessage.Message controlMessage = extractSingleMessage(messages);
      throw new RuntimeException(controlMessage.toString(), throwable);
    }
  }

  private ControlMessage.Message extractSingleMessage(final Message<ControlMessage.Message> messages) {
    return messages.getData().iterator().next();
  }

  /**
   * Send: Messages sent without expecting a reply.
   * Request: Messages sent to get a reply.
   * Reply: Messages that reply to a request.
   *
   * Not sure these variable names are conventionally used in RPC frameworks...
   * Let's revisit them when we work on
   * TODO #206: Rethink/Refactor NCS as our RPC stack
   */
  enum MessageType {
    Send,
    Request,
    Reply
  }

  private MessageType getMsgType(final ControlMessage.Message controlMessage) {
    switch (controlMessage.getType()) {
      case TaskGroupStateChanged:
      case ScheduleTaskGroup:
      case PartitionStateChanged:
      case ExecutorFailed:
        return MessageType.Send;
      case RequestPartitionLocation:
        return MessageType.Request;
      case PartitionLocationInfo:
        return MessageType.Reply;
      default:
        throw new IllegalArgumentException(controlMessage.toString());
    }
  }

  private String getExecutorId(final ControlMessage.Message controlMessage) {
    switch (controlMessage.getType()) {
      case RequestPartitionLocation:
        return controlMessage.getRequestPartitionLocationMsg().getExecutorId();
      default:
        throw new IllegalArgumentException(controlMessage.toString());
    }
  }

  private long getRequestId(final ControlMessage.Message controlMessage) {
    switch (controlMessage.getType()) {
      case PartitionLocationInfo:
        return controlMessage.getPartitionLocationInfoMsg().getRequestId();
      default:
        throw new IllegalArgumentException(controlMessage.toString());
    }
  }

  private String getListenerId(final ControlMessage.Message controlMessage) {
    switch (controlMessage.getType()) {
      case TaskGroupStateChanged:
      case PartitionStateChanged:
      case RequestPartitionLocation:
      case ExecutorFailed:
        return MessageEnvironment.MASTER_MESSAGE_RECEIVER;
      case ScheduleTaskGroup:
        return MessageEnvironment.EXECUTOR_MESSAGE_RECEIVER;
      default:
        throw new IllegalArgumentException(controlMessage.toString());
    }
  }
}
