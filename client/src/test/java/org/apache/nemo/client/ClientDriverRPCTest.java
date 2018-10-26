package org.apache.nemo.client;

import org.apache.nemo.runtime.master.ClientRPC;
import org.apache.nemo.runtime.common.comm.ControlMessage;
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
