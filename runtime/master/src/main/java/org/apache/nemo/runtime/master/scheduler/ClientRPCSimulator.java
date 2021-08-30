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
package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.ClientRPC;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * This is a Simulator for Driver-side RPC.
 * It manages communication from/to Nemo Client.
 */
// TODO #XXX: This part should support XGBoost optimization.
public final class ClientRPCSimulator implements ClientRPC {

  @Inject
  public ClientRPCSimulator() {
    super();
  }

  /**
   * Registers handler for the given type of message.
   *
   * @param type    the type of message
   * @param handler handler implementation
   * @return {@code this}
   */
  @Override
  public ClientRPCSimulator registerHandler(final ControlMessage.ClientToDriverMessageType type,
                                            final EventHandler<ControlMessage.ClientToDriverMessage> handler) {
    return this;
  }

  /**
   * Shuts down the transport.
   */
  @Override
  public void shutdown() {
  }

  /**
   * Write message to client.
   *
   * @param message message to send.
   */
  @Override
  public void send(final ControlMessage.DriverToClientMessage message) {
  }
}
