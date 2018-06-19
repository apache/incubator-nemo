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
package edu.snu.nemo.driver;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.SyncStage;
import org.apache.reef.wake.remote.Encoder;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.LinkListener;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.TransportFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Driver-side RPC implementation for communication from/to Nemo Client.
 */
public final class ClientRPC {
  private static final DriverToClientMessageEncoder ENCODER = new DriverToClientMessageEncoder();
  private static final ClientRPCLinkListener LINK_LISTENER = new ClientRPCLinkListener();
  private static final int RETRY_COUNT = 10;
  private static final int RETRY_TIMEOUT = 100;

  private final Map<ControlMessage.ClientToDriverMessageType, EventHandler<ControlMessage.ClientToDriverMessage>>
      handlers = new ConcurrentHashMap<>();
  private final Transport transport;
  private final Link<ControlMessage.DriverToClientMessage> link;
  private volatile boolean isClosed = false;

  @Inject
  private ClientRPC(final TransportFactory transportFactory,
                    final LocalAddressProvider localAddressProvider,
                    @Parameter(JobConf.ClientSideRPCServerHost.class) final String clientHost,
                    @Parameter(JobConf.ClientSideRPCServerPort.class) final int clientPort) throws IOException {
    transport = transportFactory.newInstance(localAddressProvider.getLocalAddress(),
        0, new SyncStage<>(new RPCEventHandler()), null, RETRY_COUNT, RETRY_TIMEOUT);
    final SocketAddress clientAddress = new InetSocketAddress(clientHost, clientPort);
    link = transport.open(clientAddress, ENCODER, LINK_LISTENER);
  }

  /**
   * Registers handler for the given type of message.
   * @param type the type of message
   * @param handler handler implementation
   * @return {@code this}
   */
  public ClientRPC registerHandler(final ControlMessage.ClientToDriverMessageType type,
                                   final EventHandler<ControlMessage.ClientToDriverMessage> handler) {
    if (handlers.putIfAbsent(type, handler) != null) {
      throw new RuntimeException(String.format("A handler for %s already registered", type));
    }
    return this;
  }

  /**
   * Shuts down the transport.
   */
  public void shutdown() {
    ensureRunning();
    try {
      transport.close();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      isClosed = true;
    }
  }

  /**
   * Write message to client.
   * @param message message to send.
   */
  public void send(final ControlMessage.DriverToClientMessage message) {
    ensureRunning();
    link.write(message);
  }

  /**
   * Handles message from client.
   * @param message message to process
   */
  private void handleMessage(final ControlMessage.ClientToDriverMessage message) {
    final ControlMessage.ClientToDriverMessageType type = message.getType();
    final EventHandler<ControlMessage.ClientToDriverMessage> handler = handlers.get(type);
    if (handler == null) {
      throw new RuntimeException(String.format("Handler for message type %s not registered", type));
    } else {
      handler.onNext(message);
    }
  }

  /**
   * Provides event handler for messages from client.
   */
  private final class RPCEventHandler implements EventHandler<TransportEvent> {
    @Override
    public void onNext(final TransportEvent transportEvent) {
      try {
        final byte[] data = transportEvent.getData();
        final ControlMessage.ClientToDriverMessage message = ControlMessage.ClientToDriverMessage.parseFrom(data);
        handleMessage(message);
      } catch (final InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Ensure the Transport is running.
   */
  private void ensureRunning() {
    if (isClosed) {
      throw new RuntimeException("The ClientRPC is already closed");
    }
  }

  /**
   * Provides encoder for {@link edu.snu.nemo.runtime.common.comm.ControlMessage.DriverToClientMessage}.
   */
  private static final class DriverToClientMessageEncoder implements Encoder<ControlMessage.DriverToClientMessage> {
    @Override
    public byte[] encode(final ControlMessage.DriverToClientMessage driverToClientMessage) {
      return driverToClientMessage.toByteArray();
    }
  }

  /**
   * Provides {@link LinkListener}.
   */
  private static final class ClientRPCLinkListener implements LinkListener<ControlMessage.DriverToClientMessage> {

    @Override
    public void onSuccess(final ControlMessage.DriverToClientMessage driverToClientMessage) {
    }

    @Override
    public void onException(final Throwable throwable,
                            final SocketAddress socketAddress,
                            final ControlMessage.DriverToClientMessage driverToClientMessage) {
      throw new RuntimeException(throwable);
    }
  }
}
