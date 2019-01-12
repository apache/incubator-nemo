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
import org.apache.nemo.runtime.common.message.MessageContext;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reply a response message using grpc response observer {@link StreamObserver}.
 */
final class GrpcMessageContext implements MessageContext {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcMessageContext.class);

  private final StreamObserver<ControlMessage.Message> responseObserver;

  /**
   * Constructor.
   * @param responseObserver observer for the message.
   */
  GrpcMessageContext(final StreamObserver<ControlMessage.Message> responseObserver) {
    this.responseObserver = responseObserver;
  }

  @Override
  public <U> void reply(final U replyMessage) {
    final ControlMessage.Message message = (ControlMessage.Message) replyMessage;
    LOG.debug("[REQUEST] response msg.id={}, msg.listenerId={}, msg.type={}",
        message.getId(), message.getListenerId(), message.getType());
    responseObserver.onNext((ControlMessage.Message) replyMessage);
    responseObserver.onCompleted();
  }
}
