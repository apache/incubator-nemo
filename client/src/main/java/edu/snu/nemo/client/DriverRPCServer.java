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

import com.google.protobuf.InvalidProtocolBufferException;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.SyncStage;
import org.apache.reef.wake.remote.RemoteConfiguration;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.netty.NettyMessagingTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.Map;

/**
 * Client-side RPC implementation for communication from/to Nemo Driver.
 */
@ClientSide
@NotThreadSafe
public final class DriverRPCServer {
  private final Map<ControlMessage.DriverToClientMessageType, EventHandler<ControlMessage.DriverToClientMessage>>
      handlers = new HashMap<>();
  private boolean isRunning = false;
  private boolean isShutdown = false;
  private Transport transport;
  private Link link;
  private String host;

  private static final Logger LOG = LoggerFactory.getLogger(DriverRPCServer.class);

  /**
   * Registers handler for the given type of message.
   * @param type the type of message
   * @param handler handler implementation
   * @return {@code this}
   */
  public DriverRPCServer registerHandler(final ControlMessage.DriverToClientMessageType type,
                                         final EventHandler<ControlMessage.DriverToClientMessage> handler) {
    // Registering a handler after running the server is considered not a good practice.
    ensureServerState(false);
    if (handlers.putIfAbsent(type, handler) != null) {
      throw new RuntimeException(String.format("A handler for %s already registered", type));
    }
    return this;
  }

  /**
   * Runs the RPC server.
   * Specifically, creates a {@link NettyMessagingTransport} and binds it to a listening port.
   */
  public void run() {
    // Calling 'run' multiple times is considered invalid, since it will override state variables like
    // 'transport', and 'host'.
    ensureServerState(false);
    try {
      final Injector injector = Tang.Factory.getTang().newInjector();
      final LocalAddressProvider localAddressProvider = injector.getInstance(LocalAddressProvider.class);
      host = localAddressProvider.getLocalAddress();
      injector.bindVolatileParameter(RemoteConfiguration.HostAddress.class, host);
      injector.bindVolatileParameter(RemoteConfiguration.Port.class, 0);
      injector.bindVolatileParameter(RemoteConfiguration.RemoteServerStage.class,
          new SyncStage<>(new ServerEventHandler()));
      transport = injector.getInstance(NettyMessagingTransport.class);
      LOG.info("DriverRPCServer running at {}", transport.getListeningPort());
      isRunning = true;
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return the listening port
   */
  public int getListeningPort() {
    // We cannot determine listening port if the server is not listening.
    ensureServerState(true);
    return transport.getListeningPort();
  }

  /**
   * @return the host of the client
   */
  public String getListeningHost() {
    // Listening host is determined by LocalAddressProvider, in 'run' method.
    ensureServerState(true);
    return host;
  }

  /**
   * @return the configuration for RPC server listening information
   */
  public Configuration getListeningConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(JobConf.ClientSideRPCServerHost.class, getListeningHost())
        .bindNamedParameter(JobConf.ClientSideRPCServerPort.class, String.valueOf(getListeningPort()))
        .build();
  }

  /**
   * Sends a message to driver.
   * @param message message to send
   */
  public void send(final ControlMessage.ClientToDriverMessage message) {
    // This needs active 'link' between the driver and client.
    // For the link to be alive, the driver should connect to DriverRPCServer.
    // Thus, the server must be running to send a message to the driver.
    ensureServerState(true);
    if (link == null) {
      throw new RuntimeException("The RPC server has not discovered NemoDriver yet");
    }
    link.write(message.toByteArray());
  }

  /**
   * Shut down the server.
   */
  public void shutdown() {
    // Shutting down a 'null' transport is invalid. Also, shutting down a server for multiple times is invalid.
    ensureServerState(true);
    try {
      transport.close();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      isShutdown = true;
    }
  }

  /**
   * Handles messages from driver.
   */
  private final class ServerEventHandler implements EventHandler<TransportEvent> {
    @Override
    public void onNext(final TransportEvent transportEvent) {
      final byte[] bytes = transportEvent.getData();
      final ControlMessage.DriverToClientMessage message;
      try {
        message = ControlMessage.DriverToClientMessage.parseFrom(bytes);
      } catch (final InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }

      final ControlMessage.DriverToClientMessageType type = message.getType();

      if (type == ControlMessage.DriverToClientMessageType.DriverStarted) {
        link = transportEvent.getLink();
      }

      final EventHandler<ControlMessage.DriverToClientMessage> handler = handlers.get(type);
      if (handler == null) {
        throw new RuntimeException(String.format("Handler for message type %s not registered", type));
      } else {
        handler.onNext(message);
      }
    }
  }

  /**
   * Throws a {@link RuntimeException} if the server is shut down, or it has different state than the expected state.
   * @param running the expected state of the server
   */
  private void ensureServerState(final boolean running) {
    if (isShutdown) {
      throw new RuntimeException("The DriverRPCServer is already shutdown");
    }
    if (running != isRunning) {
      throw new RuntimeException(String.format("The DriverRPCServer is %s running", isRunning ? "already" : "not"));
    }
  }
}
