/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.common.message.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.comm.GrpcMessageService;
import org.apache.nemo.runtime.common.comm.MessageServiceGrpc;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Represent the RPC server that is responsible for responding all messages from other clients. The server tries to
 * bind to a random port, and registers the bounded ip address to the name server with the localSenderId
 * (which is defined in {@link org.apache.nemo.runtime.common.message.MessageParameters.SenderId}).
 * <p>
 * The listeners, implementations of {@link MessageListener}, should be setup on this class, and then the incoming
 * messages, which contain corresponding listener id as property, are properly dispatched to the registered
 * listeners.
 * <p>
 * The currently implemented RPC methods are send and request.
 *
 * @see org.apache.nemo.runtime.common.message.MessageSender#send(Object)
 * @see org.apache.nemo.runtime.common.message.MessageSender#request(Object)
 */
final class GrpcMessageServer {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcMessageServer.class);

  private static final int NAME_SERVER_REGISTER_RETRY_COUNT = 3;
  private static final int NAME_SERVER_REGISTER_RETRY_DELAY_MS = 100;

  private final LocalAddressProvider localAddressProvider;
  private final NameResolver nameResolver;
  private final IdentifierFactory idFactory;
  private final String localSenderId;
  private final ConcurrentMap<String, MessageListener> listenerMap;

  private Server server;

  /**
   * Constructor.
   *
   * @param localAddressProvider local address provider.
   * @param nameResolver         name resolver.
   * @param idFactory            identifier factory.
   * @param localSenderId        id of the local sender.
   */
  GrpcMessageServer(final LocalAddressProvider localAddressProvider,
                    final NameResolver nameResolver,
                    final IdentifierFactory idFactory,
                    final String localSenderId) {
    this.localAddressProvider = localAddressProvider;
    this.nameResolver = nameResolver;
    this.idFactory = idFactory;
    this.localSenderId = localSenderId;
    this.listenerMap = new ConcurrentHashMap<>();
  }

  /**
   * Set up a listener.
   *
   * @param listenerId id of the listener.
   * @param listener   the message listener.
   */
  void setupListener(final String listenerId, final MessageListener<ControlMessage.Message> listener) {
    if (listenerMap.putIfAbsent(listenerId, listener) != null) {
      throw new RuntimeException("A listener for " + listenerId + " was already setup");
    }
  }

  /**
   * Remove a listener by its id.
   *
   * @param listenerId id of the listener to remove.
   */
  void removeListener(final String listenerId) {
    listenerMap.remove(listenerId);
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
    Runtime.getRuntime().addShutdownHook(new Thread(() -> GrpcMessageServer.this.server.shutdown()));

    // 3. Register the bounded ip address to name server
    registerToNameServer(server.getPort());
  }

  /**
   * For registering to the name server.
   *
   * @param port port of the socket address.
   * @throws Exception exception.
   */
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

    throw new Exception("Failed to register id=" + localSenderId + " after "
      + NAME_SERVER_REGISTER_RETRY_COUNT + " retries");
  }

  /**
   * Closes the server.
   *
   * @throws Exception exception while closing.
   */
  void close() throws Exception {
    server.shutdown();
  }

  /**
   * An implementation of send and request defined on the service of protobuf file.
   */
  private class MessageService extends MessageServiceGrpc.MessageServiceImplBase {

    private final GrpcMessageService.Void voidMessage = GrpcMessageService.Void.newBuilder().build();

    /**
     * Receive a message from a client, notify a corresponding listener, if exists, and finish the rpc call by calling
     * {@link StreamObserver#onNext(Object)} with the VOID_MESSAGE and calling {@link StreamObserver#onCompleted()}.
     *
     * @param message          a message from a client
     * @param responseObserver an observer to control this rpc call
     */
    @Override
    public void send(final ControlMessage.Message message,
                     final StreamObserver<GrpcMessageService.Void> responseObserver) {
      final MessageListener<ControlMessage.Message> listener = listenerMap.get(message.getListenerId());
      if (listener == null) {
        LOG.warn("A msg is ignored since there is no registered listener. msg.id={}, msg.listenerId={}, msg.type={}",
          message.getId(), message.getListenerId(), message.getType());
        responseObserver.onNext(voidMessage);
        responseObserver.onCompleted();
        return;
      }

      LOG.debug("[SEND] request msg.id={}, msg.listenerId={}, msg.type={}",
        message.getId(), message.getListenerId(), message.getType());
      listener.onMessage(message);
      responseObserver.onNext(voidMessage);
      responseObserver.onCompleted();
    }

    /**
     * Receive a message from a client, and notify a corresponding listener. If the listener is not registered, it
     * raises an exception with {@link StreamObserver#onError(Throwable)}. This rpc call will be finished in
     * {@link GrpcMessageContext} since the context only know when the {@link MessageListener} would reply a message.
     *
     * @param message          a message from a client
     * @param responseObserver an observer to control this rpc call
     */
    @Override
    public void request(final ControlMessage.Message message,
                        final StreamObserver<ControlMessage.Message> responseObserver) {
      LOG.debug("[REQUEST] request msg.id={}, msg.listenerId={}, msg.type={}",
        message.getId(), message.getListenerId(), message.getType());

      final MessageListener<ControlMessage.Message> listener = listenerMap.get(message.getListenerId());
      if (listener == null) {
        LOG.warn("A message arrived, which has no registered listener. msg.id={}, msg.listenerId={}, msg.type={}",
          message.getId(), message.getListenerId(), message.getType());

        responseObserver.onError(new Exception("There is no registered listener id=" + message.getListenerId()
          + " for message type=" + message.getType()));
        return;
      }

      final GrpcMessageContext messageContext = new GrpcMessageContext(responseObserver);
      // responseObserver.onComplete() is called in GrpcMessageContext, when the caller replies with a response message.
      listener.onMessageWithContext(message, messageContext);
    }
  }
}
