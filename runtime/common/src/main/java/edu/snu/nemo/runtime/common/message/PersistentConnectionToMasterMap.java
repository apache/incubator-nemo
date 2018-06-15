/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.runtime.common.message;

import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.common.exception.NodeConnectionException;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Persistent connection for sending messages to master.
 */
public final class PersistentConnectionToMasterMap {
  private final Map<String, MessageSender<ControlMessage.Message>> messageSenders;
  private final MessageEnvironment messageEnvironment;

  @Inject
  public PersistentConnectionToMasterMap(final MessageEnvironment messageEnvironment) {
    this.messageEnvironment = messageEnvironment;
    messageSenders = new HashMap<>();
    try {
      // Connect the globally known message listener IDs.
      messageSenders.put(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID,
          messageEnvironment.<ControlMessage.Message>asyncConnect(MessageEnvironment.MASTER_COMMUNICATION_ID,
              MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID).get());
      messageSenders.put(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID,
          messageEnvironment.<ControlMessage.Message>asyncConnect(MessageEnvironment.MASTER_COMMUNICATION_ID,
              MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID).get());
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
  public synchronized MessageSender<ControlMessage.Message> getMessageSender(final String listenerId) {
    final MessageSender<ControlMessage.Message> messageSender = messageSenders.get(listenerId);
    if (messageSender != null) {
      return messageSender;
    } else { // Unknown message listener.
      final MessageSender<ControlMessage.Message> createdMessageSender;
      try {
        createdMessageSender = messageEnvironment.<ControlMessage.Message>asyncConnect(
            MessageEnvironment.MASTER_COMMUNICATION_ID, listenerId).get();
        messageSenders.put(listenerId, createdMessageSender);
      } catch (InterruptedException | ExecutionException e) {
        Thread.currentThread().interrupt();
        throw new NodeConnectionException(e);
      }
      return createdMessageSender;
    }
  }
}
