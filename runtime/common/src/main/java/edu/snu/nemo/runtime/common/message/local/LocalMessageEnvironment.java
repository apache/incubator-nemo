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
package edu.snu.nemo.runtime.common.message.local;

import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.MessageListener;
import edu.snu.nemo.runtime.common.message.MessageSender;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A simple {@link MessageEnvironment} implementation that works on a single node.
 * Used for unit tests.
 */
public final class LocalMessageEnvironment implements MessageEnvironment {

  private final String currentNodeId;
  private final LocalMessageDispatcher dispatcher;

  public LocalMessageEnvironment(final String currentNodeId,
                                 final LocalMessageDispatcher dispatcher) {
    this.currentNodeId = currentNodeId;
    this.dispatcher = dispatcher;
  }

  @Override
  public <T> void setupListener(
      final String listenerId, final MessageListener<T> listener) {
    dispatcher.setupListener(currentNodeId, listenerId, listener);
  }

  @Override
  public void removeListener(final String listenerId) {
    dispatcher.removeListener(currentNodeId, listenerId);
  }

  @Override
  public <T> Future<MessageSender<T>> asyncConnect(
      final String targetId, final String messageTypeId) {
    return CompletableFuture.completedFuture(new LocalMessageSender<T>(
        currentNodeId, targetId, messageTypeId, dispatcher));
  }

  public String getId() {
    return currentNodeId;
  }

  @Override
  public void close() {
    // No-ops.
  }
}
