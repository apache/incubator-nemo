package edu.snu.onyx.runtime.common.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import java.net.InetSocketAddress;

public final class GrpcUtil {
  public final static String MASTER_GRPC_SERVER_ID = "_MASTER_GRPC_SERVER_"; // should not conflict with executor ids.

  public static ManagedChannel buildChannel(final NameResolver nameResolver,
                                            final IdentifierFactory idFactory,
                                            final String grpcServerId) throws Exception {
    // 1. Look-up destination ip address using receiver id
    final Identifier identifier = idFactory.getNewInstance(grpcServerId);
    final InetSocketAddress ipAddress = nameResolver.lookup(identifier);

    // 2. Connect to the address
    return ManagedChannelBuilder.forAddress(ipAddress.getHostName(), ipAddress.getPort()).build();
  }
}
