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
package edu.snu.nemo.client;

import edu.snu.nemo.driver.ClientRPC;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * Test for communication between {@link DriverRPCServer} and {@link ClientRPC}.
 */
public final class ClientDriverRPCTest {
  private DriverRPCServer driverRPCServer;
  private ClientRPC clientRPC;
  @Before
  public void setupDriverRPCServer() {
    // Initialize DriverRPCServer.
    driverRPCServer = new DriverRPCServer();
  }

  private void setupClientRPC() throws InjectionException {
    driverRPCServer.run();
    final Injector clientRPCInjector = Tang.Factory.getTang().newInjector(driverRPCServer.getListeningConfiguration());
    clientRPC = clientRPCInjector.getInstance(ClientRPC.class);
  }

  @After
  public void cleanup() {
    driverRPCServer.shutdown();
    clientRPC.shutdown();
  }

  /**
   * Test with empty set of handlers.
   * @throws InjectionException on Exceptions on creating {@link ClientRPC}.
   */
  @Test
  public void testRPCSetup() throws InjectionException {
    setupClientRPC();
  }

  /**
   * Test with basic request method from driver to client.
   * @throws InjectionException on Exceptions on creating {@link ClientRPC}.
   * @throws InterruptedException when interrupted while waiting EventHandler invocation
   */
  @Test
  public void testDriverToClientMethodInvocation() throws InjectionException, InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);
    driverRPCServer.registerHandler(ControlMessage.DriverToClientMessageType.DriverStarted,
        msg -> latch.countDown());
    setupClientRPC();
    clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
        .setType(ControlMessage.DriverToClientMessageType.DriverStarted).build());
    latch.await();
  }

  /**
   * Test with request-response RPC between client and driver.
   * @throws InjectionException on Exceptions on creating {@link ClientRPC}.
   * @throws InterruptedException when interrupted while waiting EventHandler invocation
   */
  @Test
  public void testBetweenClientAndDriver() throws InjectionException, InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);
    driverRPCServer.registerHandler(ControlMessage.DriverToClientMessageType.DriverStarted,
        msg -> driverRPCServer.send(ControlMessage.ClientToDriverMessage.newBuilder()
            .setType(ControlMessage.ClientToDriverMessageType.LaunchDAG)
            .setLaunchDAG(ControlMessage.LaunchDAGMessage.newBuilder().setDag("").build())
            .build()));
    setupClientRPC();
    clientRPC.registerHandler(ControlMessage.ClientToDriverMessageType.LaunchDAG, msg -> latch.countDown());
    clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
        .setType(ControlMessage.DriverToClientMessageType.DriverStarted).build());
    latch.await();
  }
}
