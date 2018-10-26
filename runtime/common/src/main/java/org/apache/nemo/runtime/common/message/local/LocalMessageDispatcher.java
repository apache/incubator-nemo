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
package org.apache.nemo.runtime.common.message.local;

import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.common.message.MessageSender;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Dispatch messages on a single machine.
 */
public final class LocalMessageDispatcher {
  private static final Tang TANG = Tang.Factory.getTang();

  private final ConcurrentMap<String, ConcurrentMap<String, MessageListener>> nodeIdToMessageListenersMap;

  @Inject
  private LocalMessageDispatcher() {
    this.nodeIdToMessageListenersMap = new ConcurrentHashMap<>();
  }

  <T> MessageSender<T> setupListener(
      final String currentNodeId, final String messageTypeId, final MessageListener<T> listener) {

    ConcurrentMap<String, MessageListener> messageTypeToListenerMap = nodeIdToMessageListenersMap.get(currentNodeId);

    if (messageTypeToListenerMap == null) {
      messageTypeToListenerMap = new ConcurrentHashMap<>();
      final ConcurrentMap<String, MessageListener> map = nodeIdToMessageListenersMap.putIfAbsent(
          currentNodeId, messageTypeToListenerMap);
      if (map != null) {
        messageTypeToListenerMap = map;
      }
    }

    if (messageTypeToListenerMap.putIfAbsent(messageTypeId, listener) != null) {
      throw new LocalDispatcherException(
          messageTypeId + " was already used in " + currentNodeId);
    }

    return new LocalMessageSender<>(currentNodeId, currentNodeId, messageTypeId, this);
  }

  void removeListener(final String currentNodeId,
                      final String listenerId) {
    nodeIdToMessageListenersMap.get(currentNodeId).remove(listenerId);
  }

  <T> void dispatchSendMessage(
      final String targetId, final String messageTypeId, final T message) {
    final MessageListener listener = nodeIdToMessageListenersMap.get(targetId).get(messageTypeId);
    if (listener == null) {
      throw new LocalDispatcherException("There was no set up listener for " + messageTypeId + " in " + targetId);
    }
    listener.onMessage(message);
  }

  <T, U> CompletableFuture<U> dispatchRequestMessage(
      final String senderId, final String targetId, final String messageTypeId, final T message) {

    final MessageListener listener = nodeIdToMessageListenersMap.get(targetId).get(messageTypeId);
    if (listener == null) {
      throw new LocalDispatcherException("There was no set up listener for " + messageTypeId + " in " + targetId);
    }

    final LocalMessageContext context = new LocalMessageContext(senderId);
    listener.onMessageWithContext(message, context);

    final Optional<Object> replyMessage = context.getReplyMessage();

    return CompletableFuture.completedFuture((U) replyMessage.orElse(null));
  }

  /**
   * A runtime exception in {@link LocalMessageDispatcher}.
   */
  private final class LocalDispatcherException extends RuntimeException {
    LocalDispatcherException(final String message) {
      super(message);
    }
  }

  /**
   * @return an {@link Injector} which has {@link LocalMessageDispatcher} for testing.
   * @throws InjectionException when failed to inject {@link LocalMessageDispatcher}
   */
  public static Injector getInjector() throws InjectionException {
    return forkInjector(TANG.newInjector());
  }

  /**
   * @param baseInjector base {@link Injector} to extend upon
   * @return an {@link Injector} which has {@link LocalMessageDispatcher} for testing.
   * @throws InjectionException when failed to inject {@link LocalMessageDispatcher}
   */
  public static Injector forkInjector(final Injector baseInjector) throws InjectionException {
    final Injector injector = baseInjector
        .forkInjector(LocalMessageEnvironment.LOCAL_MESSAGE_ENVIRONMENT_CONFIGURATION);
    injector.getInstance(LocalMessageDispatcher.class);
    return injector;
  }
}
