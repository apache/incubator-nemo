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

import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageSender;

import java.util.concurrent.CompletableFuture;

/**
 * An implementation of {@link MessageSender} which uses grpc framework internally by wrapping
 * an instance of {@link GrpcMessageClient}.
 */
final class GrpcMessageSender implements MessageSender<ControlMessage.Message> {

  private final String receiverId;
  private final String listenerId;
  private final GrpcMessageClient grpcClient;

  /**
   * Constructor.
   *
   * @param receiverId id of the receiver.
   * @param listenerId id of the listener.
   * @param grpcClient grpc message client.
   */
  GrpcMessageSender(final String receiverId,
                    final String listenerId,
                    final GrpcMessageClient grpcClient) {
    this.receiverId = receiverId;
    this.listenerId = listenerId;
    this.grpcClient = grpcClient;
  }

  @Override
  public void send(final ControlMessage.Message message) {
    grpcClient.send(message);
  }

  @Override
  public CompletableFuture<ControlMessage.Message> request(final ControlMessage.Message message) {
    return grpcClient.request(message);
  }

  @Override
  public void close() throws Exception {
    grpcClient.close();
  }
}
