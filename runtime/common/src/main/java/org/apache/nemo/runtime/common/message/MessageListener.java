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
package org.apache.nemo.runtime.common.message;


/**
 * Handles messages from {@link MessageSender}. Multiple MessageListeners can be setup using {@link MessageEnvironment}
 * while they are identified by their unique message type ids.
 *
 * @param <T> message type
 */
public interface MessageListener<T> {

  /**
   * Called back when a message is received.
   * @param message a message
   */
  void onMessage(T message);

  /**
   * Called back when a message is received, and return a response using {@link MessageContext}.
   * @param message a message
   * @param messageContext a message context
   */
  void onMessageWithContext(T message, MessageContext messageContext);

}
