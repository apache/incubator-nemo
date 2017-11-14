package edu.snu.onyx.runtime.common.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

/**
 * Represent a single RPC client to a specific server. It firstly looks up the name server to resolve
 * ip address of the target receiver, and then tries to connect to that receiver. After the connection established,
 * callers can communicates with the receiver with two methods, send and request.
 */
public final class GrpcClient {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcClient.class);

  private final NameResolver nameResolver;
  private final IdentifierFactory idFactory;
  private final String receiverId;

  private ManagedChannel managedChannel;
  private MessageServiceGrpc.MessageServiceBlockingStub blockingStub;
  private MessageServiceGrpc.MessageServiceStub asyncStub;

  GrpcClient(final NameResolver nameResolver,
             final IdentifierFactory idFactory,
             final String receiverId) {
    this.nameResolver = nameResolver;
    this.idFactory = idFactory;
    this.receiverId = receiverId;
  }

  /**
   * Find receiver's ip address using receiverId and the name server, and try to connect to the receiver.
   *
   * @throws Exception if it fails to resolve receiver's ip from the name server, or to establish connection using grpc
   */
  void connect() throws Exception {
    // 1. Look-up destination ip address using receiver id
    final Identifier identifier = idFactory.getNewInstance(receiverId);
    final InetSocketAddress ipAddress = nameResolver.lookup(identifier);

    // 2. Connect to the address
    setupChannel(ipAddress);
  }

  private void setupChannel(final InetSocketAddress ipAddress) throws Exception {
      this.managedChannel = ManagedChannelBuilder.forAddress(ipAddress.getHostName(), ipAddress.getPort())
          .usePlaintext(true)
          .build();
      this.blockingStub = MessageServiceGrpc.newBlockingStub(managedChannel);
      this.asyncStub = MessageServiceGrpc.newStub(managedChannel);
  }

  /**
   * Issue {@link edu.snu.onyx.runtime.common.message.MessageSender#send(Object)} rpc call.
   *
   * @param message a message to send
   */
  void send(final ControlMessage.Message message) {
    LOG.debug("[SEND] request msg.id={}, msg.listenerId={}, msg.type={}",
        message.getId(), message.getListenerId(), message.getType());
    try {
      blockingStub.send(message);
    } catch (final StatusRuntimeException e) {
      LOG.warn("RPC send call failed with msg.id={}, msg.listenerId={}, msg.type={}, e.cause={}, e.message={}",
          message.getId(), message.getListenerId(), message.getType(), e.getCause(), e.getMessage());
    }
  }

  /**
   * Issue {@link edu.snu.onyx.runtime.common.message.MessageSender#request(Object)} rpc call.
   *
   * @param message a message to request
   * @return a future containing response message
   */
  CompletableFuture<ControlMessage.Message> request(final ControlMessage.Message message) {
    LOG.debug("[REQUEST] request msg.id={}, msg.listenerId={}, msg.type={}",
        message.getId(), message.getListenerId(), message.getType());

    final CompletableFuture<ControlMessage.Message> completableFuture = new CompletableFuture<>();
    asyncStub.request(message, new StreamObserver<ControlMessage.Message>() {
      @Override
      public void onNext(final ControlMessage.Message responseMessage) {
        LOG.debug("[REQUEST] response msg.id={}, msg.listenerId={}, msg.type={}",
            responseMessage.getId(), responseMessage.getListenerId(), responseMessage.getType());
        completableFuture.complete(responseMessage);
      }

      @Override
      public void onError(final Throwable e) {
        LOG.warn("RPC request call failed with msg.id={}, msg.listenerId={}, msg.type={}, e.cause={}, e.message={}",
            message.getId(), message.getListenerId(), message.getType(), e.getCause(), e.getMessage());
        completableFuture.completeExceptionally(e);
      }

      @Override
      public void onCompleted() {
        LOG.debug("[REQUEST] completed. msg.id={}, msg.listenerId={}, msg.type={}",
            message.getId(), message.getListenerId(), message.getType());
      }
    });

    return completableFuture;
  }

  void close() throws Exception {
    managedChannel.shutdown();
  }
}
