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

import edu.snu.nemo.runtime.common.message.MessageContext;

import java.util.Optional;

/**
 * A simple {@link MessageContext} implementation that works on a single node.
 */
final class LocalMessageContext implements MessageContext {

  private final String senderId;
  private Object replyMessage;

  /**
   *  TODO #10: Handle Method Javadocs Requirements for Checkstyle Warnings.
   * @param senderId  TODO #10: Handle Method Javadocs Requirements for Checkstyle Warnings.
   */
  LocalMessageContext(final String senderId) {
    this.senderId = senderId;
  }

  public String getSenderId() {
    return senderId;
  }

  @Override
  public <T> void reply(final T message) {
    this.replyMessage = message;
  }

  /**
   *  TODO #10: Handle Method Javadocs Requirements for Checkstyle Warnings.
   * @return TODO #10: Handle Method Javadocs Requirements for Checkstyle Warnings.
   */
  public Optional<Object> getReplyMessage() {
    return Optional.ofNullable(replyMessage);
  }
}
