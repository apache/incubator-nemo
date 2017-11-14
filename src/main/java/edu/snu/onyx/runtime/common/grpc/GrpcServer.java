package edu.snu.onyx.runtime.common.grpc;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.Closeable;
import java.net.InetSocketAddress;

public final class GrpcServer implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class);

  private static final int NAME_SERVER_REGISTER_RETRY_COUNT = 3;
  private static final int NAME_SERVER_REGISTER_RETRY_DELAY_MS = 100;

  private final LocalAddressProvider localAddressProvider;
  private final NameResolver nameResolver;
  private final IdentifierFactory idFactory;

  private Server server;

  @Inject
  public GrpcServer(final LocalAddressProvider localAddressProvider,
                    final NameResolver nameResolver,
                    final IdentifierFactory idFactory) {
    this.localAddressProvider = localAddressProvider;
    this.nameResolver = nameResolver;
    this.idFactory = idFactory;
  }

  public void start(final String grpcServerId, final BindableService... services) throws Exception {
    // 1. Bind to random port.
    final ServerBuilder serverBuilder = ServerBuilder.forPort(0);
    for (final BindableService service : services) {
      serverBuilder.addService(service);
    }
    this.server = serverBuilder.build();

    // 2. Start the server.
    server.start();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> GrpcServer.this.server.shutdown()));

    // 3. Register the bounded ip address to name server.
    final InetSocketAddress socketAddress =
        new InetSocketAddress(localAddressProvider.getLocalAddress(), server.getPort());
    for (int i = 0; i < NAME_SERVER_REGISTER_RETRY_COUNT; i++) {
      try {
        nameResolver.register(idFactory.getNewInstance(grpcServerId), socketAddress);
        return;
      } catch (final Exception e) {
        LOG.warn("Exception occurred while registering a server to the name server. id=" + grpcServerId, e);
        Thread.sleep(NAME_SERVER_REGISTER_RETRY_DELAY_MS);
      }
    }

    throw new Exception(
        "Failed to register id=" + grpcServerId + " after " + NAME_SERVER_REGISTER_RETRY_COUNT + " retries");
  }

  @Override
  public void close() {
    server.shutdown();
  }
}
