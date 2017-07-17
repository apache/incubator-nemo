package edu.snu.vortex.runtime.common.message.ncs;

import edu.snu.vortex.runtime.common.ReplyFutureMap;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageSender;
import org.apache.reef.io.network.Connection;

import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MessageSender for NCS.
 * TODO #206: Rethink/Refactor NCS as our RPC stack
 */
final class NcsMessageSender implements MessageSender<ControlMessage.Message> {
  private static final Logger LOG = Logger.getLogger(NcsMessageSender.class.getName());

  private final Connection<ControlMessage.Message> connection;
  private final ReplyFutureMap<ControlMessage.Message> replyFutureMap;

  NcsMessageSender(
      final Connection<ControlMessage.Message> connection,
      final ReplyFutureMap replyFutureMap) {
    this.connection = connection;
    this.replyFutureMap = replyFutureMap;
  }

  @Override
  public void send(final ControlMessage.Message message) {
    LOG.log(Level.FINE, "send: {0}", message);
    connection.write(message);
  }

  @Override
  public CompletableFuture<ControlMessage.Message> request(final ControlMessage.Message message) {
    LOG.log(Level.FINE, "request: {0}", message);
    final CompletableFuture<ControlMessage.Message> future = replyFutureMap.beforeRequest(message.getId());
    connection.write(message);
    return future;
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }
}
