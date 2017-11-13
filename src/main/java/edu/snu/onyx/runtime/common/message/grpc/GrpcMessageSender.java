package edu.snu.onyx.runtime.common.message.grpc;

import edu.snu.onyx.runtime.common.comm.ControlMessage;
import edu.snu.onyx.runtime.common.message.MessageSender;

import java.util.concurrent.CompletableFuture;

/**
 * An implementation of {@link MessageSender} which uses grpc framework internally by wrapping
 * an instance of {@link GrpcMessageClient}.
 */
final class GrpcMessageSender implements MessageSender<ControlMessage.Message> {

  private final String receiverId;
  private final String listenerId;
  private final GrpcMessageClient grpcClient;

  GrpcMessageSender(final String receiverId,
                    final String listenerId,
                    final GrpcMessageClient grpcClient) {
    this.receiverId = receiverId;
    this.listenerId = listenerId;
    this.grpcClient = grpcClient;
  }

  @Override
  public void send(final ControlMessage.Message message) {
    grpcClient.send(message);
  }

  @Override
  public CompletableFuture<ControlMessage.Message> request(final ControlMessage.Message message) {
    return grpcClient.request(message);
  }

  @Override
  public void close() throws Exception {
    grpcClient.close();
  }
}
