package org.apache.nemo.runtime.common.message.ncs;

import org.apache.nemo.runtime.common.message.MessageContext;
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
    final Connection connection = connectionFactory.newConnection(idFactory.getNewInstance(senderId));
    try {
      connection.open();
      connection.write(replyMessage);
      // We do not call connection.close since NCS caches connection.
      // Disabling Sonar warning (squid:S2095)
    } catch (final NetworkException e) {
      // TODO #140: Properly classify and handle each RPC failure
      // Not logging the stacktrace here, as it's not very useful.
      LOG.error("NCS Exception");
    }
  }
}
