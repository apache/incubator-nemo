package edu.snu.nemo.runtime.common.message.grpc;

import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageContext;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reply a response message using grpc response observer {@link StreamObserver}.
 */
final class GrpcMessageContext implements MessageContext {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcMessageContext.class);

  private final StreamObserver<ControlMessage.Message> responseObserver;

  /**
   * Constructor.
   * @param responseObserver observer for the message.
   */
  GrpcMessageContext(final StreamObserver<ControlMessage.Message> responseObserver) {
    this.responseObserver = responseObserver;
  }

  @Override
  public <U> void reply(final U replyMessage) {
    final ControlMessage.Message message = (ControlMessage.Message) replyMessage;
    LOG.debug("[REQUEST] response msg.id={}, msg.listenerId={}, msg.type={}",
        message.getId(), message.getListenerId(), message.getType());
    responseObserver.onNext((ControlMessage.Message) replyMessage);
    responseObserver.onCompleted();
  }
}
