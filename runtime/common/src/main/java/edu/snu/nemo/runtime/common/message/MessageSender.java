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

import java.util.concurrent.CompletableFuture;

/**
 * This class sends messages to {@link MessageListener} with some defined semantics.
 * @param <T> message type
 */
public interface MessageSender<T> {

  /**
   * Send a message to corresponding {@link MessageListener#onMessage}. It does not guarantee whether
   * the message is sent successfully or not.
   *
   * @param message a message
   */
  void send(T message);

  /**
   * Send a message to corresponding {@link MessageListener#onMessageWithContext} and return
   * a reply message. If there was an exception, the returned future would be failed.
   *
   * @param message a message
   * @param <U> reply message type.
   * @return a future
   */
  <U> CompletableFuture<U> request(T message);

  /**
   * Closes the connection.
   * @throws Exception while closing.
   */
  void close() throws Exception;
}
