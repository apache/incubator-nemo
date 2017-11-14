package edu.snu.onyx.runtime.common.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public final class GrpcServer {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class);

  private static final int NAME_SERVER_REGISTER_RETRY_COUNT = 3;
  private static final int NAME_SERVER_REGISTER_RETRY_DELAY_MS = 100;

  private final LocalAddressProvider localAddressProvider;
  private final NameResolver nameResolver;
  private final IdentifierFactory idFactory;
  private final String localSenderId;

  private Server server;

  GrpcServer(final LocalAddressProvider localAddressProvider,
             final NameResolver nameResolver,
             final IdentifierFactory idFactory,
             final String localSenderId) {
    this.localAddressProvider = localAddressProvider;
    this.nameResolver = nameResolver;
    this.idFactory = idFactory;
    this.localSenderId = localSenderId;
  }

  /**
   * This method starts a {@link Server} with random port, and register the ip address with the local sender id
   * to the name server.
   *
   * @throws Exception when any network exception occur during starting procedure
   */
  void start() throws Exception {
    // 1. Bind to random port
    this.server = ServerBuilder.forPort(0)
        .addService(new MessageService())
        .build();

    // 2. Start the server
    server.start();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> GrpcServer.this.server.shutdown()));

    // 3. Register the bounded ip address to name server
    registerToNameServer(server.getPort());
  }

  private void registerToNameServer(final int port) throws Exception {
    final InetSocketAddress socketAddress = new InetSocketAddress(localAddressProvider.getLocalAddress(), port);
    for (int i = 0; i < NAME_SERVER_REGISTER_RETRY_COUNT; i++) {
      try {
        nameResolver.register(idFactory.getNewInstance(localSenderId), socketAddress);
        return;
      } catch (final Exception e) {
        LOG.warn("Exception occurred while registering a server to the name server. id=" + localSenderId, e);
        Thread.sleep(NAME_SERVER_REGISTER_RETRY_DELAY_MS);
      }
    }

    throw new Exception(
        "Failed to register id=" + localSenderId + " after " + NAME_SERVER_REGISTER_RETRY_COUNT + " retries");
  }

  void close() throws Exception {
    server.shutdown();
  }
}
