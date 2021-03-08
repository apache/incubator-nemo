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
package org.apache.nemo.runtime.common.message;

import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.grpc.GrpcMessageClient;
import org.apache.nemo.runtime.common.message.grpc.GrpcMessageEnvironment;
import org.apache.nemo.runtime.common.message.grpc.GrpcMessageSender;
import org.apache.nemo.runtime.common.message.grpc.GrpcMessageServer;
import org.apache.nemo.runtime.common.message.ncs.NcsMessageEnvironment;
import org.apache.nemo.runtime.common.message.ncs.NcsMessageSender;
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
public final class GrpcNcsMessageEnvironment implements MessageEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcNcsMessageEnvironment.class);

  private final NcsMessageEnvironment ncsMessageEnvironment;
  private final GrpcMessageEnvironment grpcMessageEnvironment;

  @Inject
  private GrpcNcsMessageEnvironment(
    final NcsMessageEnvironment ncsMessageEnvironment,
    final GrpcMessageEnvironment grpcMessageEnvironment) {
    this.ncsMessageEnvironment = ncsMessageEnvironment;
    this.grpcMessageEnvironment = grpcMessageEnvironment;
  }

  @Override
  public <T> void setupListener(final String listenerId, final MessageListener<T> listener) {
    grpcMessageEnvironment.setupListener(listenerId, listener);
    ncsMessageEnvironment.setupListener(listenerId, listener);
  }

  @Override
  public void removeListener(final String listenerId) {
    grpcMessageEnvironment.removeListener(listenerId);
    ncsMessageEnvironment.removeListener(listenerId);
  }

  @Override
  public <T> Future<MessageSender<T>> asyncConnect(final String receiverId, final String listenerId) {

    final CompletableFuture completableFuture = new CompletableFuture();

    final Future<MessageSender<T>> grpcFuture = grpcMessageEnvironment.asyncConnect(receiverId, listenerId);
    final Future<MessageSender<T>> ncsFuture = ncsMessageEnvironment.asyncConnect(receiverId, listenerId);

    try {
      final MessageSender<ControlMessage.Message> grpcNcsSender = new GrpcNcsMessageSender(
        (GrpcMessageSender)  grpcFuture.get(), (NcsMessageSender) ncsFuture.get());

      completableFuture.complete(grpcNcsSender);
      return completableFuture;
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    grpcMessageEnvironment.close();
    ncsMessageEnvironment.close();
  }

  @Override
  public String getId() {
    return grpcMessageEnvironment.getId();
  }
}
