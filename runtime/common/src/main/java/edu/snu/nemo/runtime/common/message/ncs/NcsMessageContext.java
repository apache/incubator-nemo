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
package edu.snu.nemo.runtime.common.message.ncs;

import edu.snu.nemo.runtime.common.message.MessageContext;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.wake.IdentifierFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Message context for NCS.
 */
final class NcsMessageContext implements MessageContext {
  private static final Logger LOG = LoggerFactory.getLogger(NcsMessageContext.class.getName());

  private final String senderId;
  private final ConnectionFactory connectionFactory;
  private final IdentifierFactory idFactory;

  NcsMessageContext(final String senderId,
                    final ConnectionFactory connectionFactory,
                    final IdentifierFactory idFactory) {
    this.senderId = senderId;
    this.connectionFactory = connectionFactory;
    this.idFactory = idFactory;
  }

  public String getSenderId() {
    return senderId;
  }

  @Override
  @SuppressWarnings("squid:S2095")
  public <U> void reply(final U replyMessage) {
    LOG.debug("[REPLY]: {}", replyMessage);
    final Connection connection = connectionFactory.newConnection(idFactory.getNewInstance(senderId));
    try {
      connection.open();
      connection.write(replyMessage);
      // We do not call connection.close since NCS caches connection.
      // Disabling Sonar warning (squid:S2095)
    } catch (final NetworkException e) {
      throw new RuntimeException("Cannot connect to " + senderId, e);
    }
  }
}
