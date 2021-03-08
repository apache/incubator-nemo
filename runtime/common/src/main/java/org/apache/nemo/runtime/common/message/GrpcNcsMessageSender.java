package org.apache.nemo.runtime.common.message;

import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.grpc.GrpcMessageClient;
import org.apache.nemo.runtime.common.message.grpc.GrpcMessageSender;
import org.apache.nemo.runtime.common.message.ncs.NcsMessageSender;

import java.util.concurrent.CompletableFuture;

public final class GrpcNcsMessageSender implements MessageSender<ControlMessage.Message> {

  private final GrpcMessageSender grpcMessageSender;
  private final NcsMessageSender ncsMessageSender;

  GrpcNcsMessageSender(final GrpcMessageSender grpcMessageSender,
                       final NcsMessageSender ncsMessageSender) {
    this.grpcMessageSender = grpcMessageSender;
    this.ncsMessageSender = ncsMessageSender;
  }

  @Override
  public void send(final ControlMessage.Message message) {
    ncsMessageSender.send(message);
  }

  @Override
  public CompletableFuture<ControlMessage.Message> request(final ControlMessage.Message message) {
    return grpcMessageSender.request(message);
  }

  @Override
  public void close() throws Exception {
    grpcMessageSender.close();
    ncsMessageSender.close();
  }
}
