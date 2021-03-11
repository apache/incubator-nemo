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

import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.common.exception.NodeConnectionException;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID;
import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.RUNTIME_MASTER_MESSAGE_LISTENER_ID;

/**
 * Persistent connection for sending messages to master.
 */
public final class PersistentConnectionToMasterMap {
  private final Map<MessageEnvironment.ListenerType, MessageSender<ControlMessage.Message>> messageSenders;
  private final MessageEnvironment messageEnvironment;

  @Inject
  private PersistentConnectionToMasterMap(final MessageEnvironment messageEnvironment) {
    this.messageEnvironment = messageEnvironment;
    messageSenders = new HashMap<>();
    try {
      // Connect the globally known message listener IDs.
      messageSenders.put(RUNTIME_MASTER_MESSAGE_LISTENER_ID,
          messageEnvironment.<ControlMessage.Message>asyncConnect(MessageEnvironment.MASTER_ID,
              RUNTIME_MASTER_MESSAGE_LISTENER_ID).get());
      messageSenders.put(BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID,
          messageEnvironment.<ControlMessage.Message>asyncConnect(MessageEnvironment.MASTER_ID,
              BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID).get());

    } catch (InterruptedException | ExecutionException e) {
      Thread.currentThread().interrupt();
      throw new NodeConnectionException(e);
    }
  }

  /**
   * Get the message sender corresponding to the given listener ID.
   *
   * @param listenerId the ID of the listener.
   * @return the message sender.
   */
  public synchronized MessageSender<ControlMessage.Message> getMessageSender(
    final MessageEnvironment.ListenerType listenerId) {

    final MessageSender<ControlMessage.Message> messageSender = messageSenders.get(listenerId);
    if (messageSender != null) {
      return messageSender;
    } else { // Unknown message listener.
      final MessageSender<ControlMessage.Message> createdMessageSender;
      try {
        createdMessageSender = messageEnvironment.<ControlMessage.Message>asyncConnect(
            MessageEnvironment.MASTER_ID, listenerId).get();
        messageSenders.put(listenerId, createdMessageSender);
      } catch (InterruptedException | ExecutionException e) {
        Thread.currentThread().interrupt();
        throw new NodeConnectionException(e);
      }
      return createdMessageSender;
    }
  }
}
