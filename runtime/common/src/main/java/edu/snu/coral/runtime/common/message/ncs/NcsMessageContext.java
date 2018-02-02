package edu.snu.coral.runtime.common.message.ncs;

import edu.snu.coral.runtime.common.message.MessageContext;
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
  public <U> void reply(final U replyMessage) {
    LOG.debug("[REPLY]: {}", replyMessage);
    final Connection connection = connectionFactory.newConnection(idFactory.getNewInstance(senderId));
    try {
      connection.open();
    } catch (final NetworkException e) {
      throw new RuntimeException("Cannot connect to " + senderId, e);
    }

    connection.write(replyMessage);
  }
}
