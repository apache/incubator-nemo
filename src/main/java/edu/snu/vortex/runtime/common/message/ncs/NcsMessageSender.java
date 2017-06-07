package edu.snu.vortex.runtime.common.message.ncs;

import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageSender;
import org.apache.reef.io.network.Connection;

import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MessageSender for NCS.
 * TODO #206: Rethink/Refactor NCS as our RPC stack
 */
final class NcsMessageSender implements MessageSender<ControlMessage.Message> {
  private static final Logger LOG = Logger.getLogger(NcsMessageSender.class.getName());

  private final Connection<ControlMessage.Message> connection;
  private final ReplyWaitingLock replyWaitingLock;

  NcsMessageSender(
      final Connection<ControlMessage.Message> connection,
      final ReplyWaitingLock replyWaitingLock) {
    this.connection = connection;
    this.replyWaitingLock = replyWaitingLock;
  }

  @Override
  public void send(final ControlMessage.Message message) {
    LOG.log(Level.FINE, "send: {0}", message);
    connection.write(message);
  }

  @Override
  public <U> Future<U> request(final ControlMessage.Message message) {
    LOG.log(Level.FINE, "request: {0}", message);
    connection.write(message);
    return (Future) replyWaitingLock.waitingReply(message.getId());
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }
}
