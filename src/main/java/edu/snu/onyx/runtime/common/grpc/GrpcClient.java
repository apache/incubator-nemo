package edu.snu.onyx.runtime.common.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.net.InetSocketAddress;

/**
 * Grpc client.
 */
@ThreadSafe
public class GrpcClient {
  private final NameResolver nameResolver;
  private final IdentifierFactory idFactory;

  @Inject
  public GrpcClient(final NameResolver nameResolver,
                    final IdentifierFactory idFactory) {
    this.nameResolver = nameResolver;
    this.idFactory = idFactory;
  }

  public synchronized ManagedChannel openChannel(final String grpcServerId) throws Exception {
    // 1. Look-up destination ip address using receiver id
    final Identifier identifier = idFactory.getNewInstance(grpcServerId);
    final InetSocketAddress ipAddress = nameResolver.lookup(identifier);

    // 2. Connect to the address
    return ManagedChannelBuilder.forAddress(ipAddress.getHostName(), ipAddress.getPort())
        .usePlaintext(true)
        .build();
  }
}
