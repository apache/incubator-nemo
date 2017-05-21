package edu.snu.vortex.runtime.common.message;

import edu.snu.vortex.runtime.common.message.ncs.NcsMessageEnvironment;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.concurrent.Future;

/**
 * Set up {@link MessageListener}s to handle incoming messages on this node, and connect to remote nodes and return
 * {@link MessageSender}s to send message to them.
 */
@DefaultImplementation(NcsMessageEnvironment.class)
public interface MessageEnvironment {

  String MASTER_COMMUNICATION_ID = "MASTER";
  String MASTER_MESSAGE_RECEIVER = "MASTER_MESSAGE_RECEIVER";
  String EXECUTOR_MESSAGE_RECEIVER = "EXECUTOR_MESSAGE_RECEIVER";

  /**
   * Set up a {@link MessageListener} with a message type id.
   *
   * @param messageTypeId an identifier of the message type which would be handled by message listener
   * @param listener a message listener
   * @param <T> The type of the message to be sent in the environment
   */
  <T> void setupListener(String messageTypeId, MessageListener<T> listener);

  /**
   * Asynchronously connect to the node called 'receiverId' and return a future of {@link MessageSender} that sends
   * messages with 'messageTypeId'.
   *
   * @param receiverId a receiver id
   * @param messageTypeId a message type id
   * @param <T> The type of the message to be sent in the environment
   * @return a message sender
   */
  <T> Future<MessageSender<T>> asyncConnect(String receiverId, String messageTypeId);

  /**
   * Return an id of current node.
   *
   * @return an identifier
   */
  String getId();

  /**
   * Close this message environment.
   * @throws Exception while closing
   */
  void close() throws Exception;
}
