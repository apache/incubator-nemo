/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.message;

import org.apache.nemo.runtime.message.netty.NettyWorkerEnvironment;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.net.InetSocketAddress;
import java.util.concurrent.Future;

/**
 * Set up {@link MessageListener}s to handle incoming messages on this node, and connect to remote nodes and return
 * {@link MessageSender}s to send message to them.
 */
@DefaultImplementation(NettyWorkerEnvironment.class)
public interface MessageEnvironment {

  public final String MASTER_ID = "master";

  // The ID of the master used for distinguish the sender or receiver.
  enum ListenerType {
    // The globally known message listener IDs.
    RUNTIME_MASTER_MESSAGE_LISTENER_ID,
    BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID,
    PIPE_MANAGER_MASTER_MESSAGE_LISTENER_ID,
    EXECUTOR_MESSAGE_LISTENER_ID,
    TASK_INDEX_MESSAGE_LISTENER_ID,
    TASK_SCHEDULE_MAP_LISTENER_ID,
    SCALEOUT_MESSAGE_LISTENER_ID,
    TRANSFER_INDEX_LISTENER_ID,
    STAGE_OFFLOADING_LISTENER_ID,
    SCALE_DECISION_MESSAGE_LISTENER_ID,
    SOURCE_EVENT_HANDLER_ID,
    RELAY_SERVER_ID,
    YARN_OFFLOADING_EXECUTOR_REQUEST_ID,
    LAMBDA_OFFLOADING_REQUEST_ID,
    NAMING_REQUEST_ID,
  }


  /**
   * Set up a {@link MessageListener} with a listener id.
   *
   * @param listenerId an identifier of the message listener
   * @param listener a message listener
   * @param <T> The type of the message to be sent in the environment
   */
  <T> void setupListener(ListenerType listenerId, MessageListener<T> listener);

  /**
   * Remove the {@link MessageListener} bound to a specific listener ID.
   *
   * @param listenerId the ID of the listener to remove.
   */
  void removeListener(ListenerType listenerId);

  /**
   * Asynchronously connect to the node called {@code receiverId} and return a future of {@link MessageSender}
   * that sends messages to the listener with {@code listenerId}.
   *
   * @param receiverId a receiver id
   * @param listenerId an identifier of the message listener
   * @param <T> The type of the message to be sent in the environment
   * @return a message sender
   */
  <T> Future<MessageSender<T>> asyncConnect(String receiverId, ListenerType listenerId);
  <T> Future<MessageSender<T>> asyncConnect(String receiverId, ListenerType listenerId,
                                            InetSocketAddress addr);

  int getPort();

  /**
   * Close this message environment.
   * @throws Exception while closing
   */
  void close() throws Exception;

  /**
   * @return identifier for this node
   */
  String getId();
}
