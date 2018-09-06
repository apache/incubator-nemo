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
package org.apache.nemo.runtime.common.message.local;

import org.apache.nemo.runtime.common.message.MessageSender;

import java.util.concurrent.CompletableFuture;

/**
 * A simple {@link MessageSender} implementation that works on a single node.
 * @param <T> a message type
 */
public final class LocalMessageSender<T> implements MessageSender<T> {

  private final String senderId;
  private final String targetId;
  private final String messageTypeId;
  private final LocalMessageDispatcher dispatcher;
  private boolean isClosed;

  public LocalMessageSender(final String senderId,
                     final String targetId,
                     final String messageTypeId,
                     final LocalMessageDispatcher dispatcher) {
    this.senderId = senderId;
    this.targetId = targetId;
    this.messageTypeId = messageTypeId;
    this.dispatcher = dispatcher;
    this.isClosed = false;
  }

  @Override
  public void send(final T message) {
    if (isClosed) {
      throw new RuntimeException("Closed");
    }
    dispatcher.dispatchSendMessage(targetId, messageTypeId, message);
  }

  @Override
  public <U> CompletableFuture<U> request(final T message) {
    if (isClosed) {
      throw new RuntimeException("Closed");
    }
    return dispatcher.dispatchRequestMessage(senderId, targetId, messageTypeId, message);
  }

  @Override
  public void close() throws Exception {
    isClosed = true;
  }
}
