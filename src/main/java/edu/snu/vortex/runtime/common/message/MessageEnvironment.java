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

  // The ID of the master used for distinguish the sender or receiver.
  String MASTER_COMMUNICATION_ID = "MASTER";
  // The globally known message listener IDs.
  String RUNTIME_MASTER_MESSAGE_LISTENER_ID = "RUNTIME_MASTER_MESSAGE_LISTENER_ID";
  String PARTITION_MANAGER_MASTER_MESSAGE_LISTENER_ID = "PARTITION_MANAGER_MASTER_MESSAGE_LISTENER_ID";
  String EXECUTOR_MESSAGE_LISTENER_ID = "EXECUTOR_MESSAGE_LISTENER_ID";

  /**
   * Set up a {@link MessageListener} with a listener id.
   *
   * @param listenerId an identifier of the message listener
   * @param listener a message listener
   * @param <T> The type of the message to be sent in the environment
   */
  <T> void setupListener(String listenerId, MessageListener<T> listener);

  /**
   * Remove the {@link MessageListener} bound to a specific listener ID.
   *
   * @param listenerId the ID of the listener to remove.
   */
  void removeListener(String listenerId);

  /**
   * Asynchronously connect to the node called {@code receiverId} and return a future of {@link MessageSender}
   * that sends messages to the listener with {@code listenerId}.
   *
   * @param receiverId a receiver id
   * @param listenerId an identifier of the message listener
   * @param <T> The type of the message to be sent in the environment
   * @return a message sender
   */
  <T> Future<MessageSender<T>> asyncConnect(String receiverId, String listenerId);

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
