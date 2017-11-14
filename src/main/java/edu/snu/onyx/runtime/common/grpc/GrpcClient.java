package edu.snu.onyx.runtime.common.grpc;

import io.grpc.ManagedChannelBuilder;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import java.net.InetSocketAddress;

/**
 * Represent a single RPC client to a specific server. It firstly looks up the name server to resolve
 * ip address of the target receiver, and then tries to connect to that receiver. After the connection established,
 * callers can communicates with the receiver with two methods, send and request.
 */
public final class GrpcClient {
  private final NameResolver nameResolver;
  private final IdentifierFactory idFactory;
  private final String receiverId;

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
    this.managedChannel = ManagedChannelBuilder.forAddress(ipAddress.getHostName(), ipAddress.getPort()).build();
  }
}
