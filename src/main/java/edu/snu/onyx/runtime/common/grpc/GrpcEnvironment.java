package edu.snu.onyx.runtime.common.grpc;

import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * This class is installed when every node, containing both driver and evaluator, has been started, and setup
 * grpc environment to implement RPC semantics defined in {@link edu.snu.onyx.runtime.common.message} package.
 * For each GrpcEnvironment, there are a single {@link GrpcServer} and multiple {@link GrpcClient},
 * which are responsible for responding replies for messages from other clients, and for transferring messages
 * to other servers, respectively.
 *
 * The {@link GrpcServer} is started as soon as the environment is initialized, and registers the unique sender
 * id of the local node to name server, which is used for id-based communication. The {@link MessageListener}s should
 * be setup to correctly handle incoming messages.
 *
 * The {@link GrpcClient}s are created whenever there is a request to create a {@link MessageSender}, a component
 * to issue RPC calls to other servers. Like the {@link GrpcServer} registers its id to the name server, the
 * {@link GrpcClient} uses target receiver id to look up the name server to resolve the ip address of the target
 * server before establishing a connection to the server.
 */
public final class GrpcEnvironment implements MessageEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcEnvironment.class);

  private final NameResolver nameResolver;
  private final IdentifierFactory idFactory;
  private final GrpcServer grpcServer;

  @Inject
  private GrpcEnvironment(
      final LocalAddressProvider localAddressProvider,
      final NameResolver nameResolver,
      final IdentifierFactory idFactory,
      @Parameter(MessageParameters.SenderId.class) final String localSenderId) {
    this.nameResolver = nameResolver;
    this.idFactory = idFactory;
    this.grpcServer = new GrpcServer(localAddressProvider, nameResolver, idFactory, localSenderId);

    try {
      this.grpcServer.start();
    } catch (final Exception e) {
      LOG.warn("Failed to start grpc server", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T> void setupListener(final String listenerId, final MessageListener<T> listener) {
    grpcServer.setupListener(listenerId, (MessageListener) listener);
  }

  @Override
  public void removeListener(final String listenerId) {
    grpcServer.removeListener(listenerId);
  }

  @Override
  public <T> Future<MessageSender<T>> asyncConnect(final String receiverId) {
    final CompletableFuture completableFuture = new CompletableFuture();

    // It can be optimized by sharing  ManagedChannel of same grpc clients connecting to the same ip address,
    // when it figured out there are significant duplicated clients should be created.
    final GrpcClient grpcClient = new GrpcClient(nameResolver, idFactory, receiverId);

    try {
      grpcClient.connect();
      final MessageSender<ControlMessage.Message> messageSender = new GrpcMessageSender(grpcClient);
      completableFuture.complete(messageSender);
    } catch (final Exception e) {
      LOG.warn("Failed to connect a receiver id=" + receiverId + ", listenerId=" + listenerId, e);
      completableFuture.completeExceptionally(e);
    }

    return completableFuture;
  }

  @Override
  public void close() throws Exception {
    grpcServer.close();
  }
}
