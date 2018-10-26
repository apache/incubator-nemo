package org.apache.nemo.runtime.common.message.ncs;

import org.apache.nemo.runtime.common.ReplyFutureMap;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageSender;
import org.apache.reef.io.network.Connection;

import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MessageSender for NCS.
 */
final class NcsMessageSender implements MessageSender<ControlMessage.Message> {
  private static final Logger LOG = LoggerFactory.getLogger(NcsMessageSender.class.getName());

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
    connection.write(message);
  }

  @Override
  public CompletableFuture<ControlMessage.Message> request(final ControlMessage.Message message) {
    final CompletableFuture<ControlMessage.Message> future = replyFutureMap.beforeRequest(message.getId());
    connection.write(message);
    return future;
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }
}
