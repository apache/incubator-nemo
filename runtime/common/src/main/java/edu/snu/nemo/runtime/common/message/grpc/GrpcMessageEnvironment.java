/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nemo.runtime.common.message.grpc;

import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.common.message.MessageParameters;
import org.apache.nemo.runtime.common.message.MessageSender;
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
 * grpc environment to implement RPC semantics defined in {@link org.apache.nemo.runtime.common.message} package.
 * For each GrpcMessageEnvironment, there are a single {@link GrpcMessageServer} and multiple {@link GrpcMessageClient},
 * which are responsible for responding replies for messages from other clients, and for transferring messages
 * to other servers, respectively.
 *
 * The {@link GrpcMessageServer} is started as soon as the environment is initialized, and registers the unique sender
 * id of the local node to name server, which is used for id-based communication. The {@link MessageListener}s should
 * be setup to correctly handle incoming messages.
 *
 * The {@link GrpcMessageClient}s are created whenever there is a request to create a {@link MessageSender}, a component
 * to issue RPC calls to other servers. Like the {@link GrpcMessageServer} registers its id to the name server, the
 * {@link GrpcMessageClient} uses target receiver id to look up the name server to resolve the ip address of the target
 * server before establishing a connection to the server.
 */
public final class GrpcMessageEnvironment implements MessageEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcMessageEnvironment.class);

  private final NameResolver nameResolver;
  private final IdentifierFactory idFactory;
  private final GrpcMessageServer grpcServer;
  private final String localSenderId;

  @Inject
  private GrpcMessageEnvironment(
      final LocalAddressProvider localAddressProvider,
      final NameResolver nameResolver,
      final IdentifierFactory idFactory,
      @Parameter(MessageParameters.SenderId.class) final String localSenderId) {
    this.nameResolver = nameResolver;
    this.idFactory = idFactory;
    this.grpcServer = new GrpcMessageServer(localAddressProvider, nameResolver, idFactory, localSenderId);
    this.localSenderId = localSenderId;

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
  public <T> Future<MessageSender<T>> asyncConnect(final String receiverId, final String listenerId) {
    final CompletableFuture completableFuture = new CompletableFuture();

    // It can be optimized by sharing  ManagedChannel of same grpc clients connecting to the same ip address,
    // when it figured out there are significant duplicated clients should be created.
    final GrpcMessageClient grpcClient = new GrpcMessageClient(nameResolver, idFactory, receiverId);

    try {
      grpcClient.connect();
      final MessageSender<ControlMessage.Message> messageSender = new GrpcMessageSender(
          receiverId, listenerId, grpcClient);
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

  @Override
  public String getId() {
    return localSenderId;
  }
}
