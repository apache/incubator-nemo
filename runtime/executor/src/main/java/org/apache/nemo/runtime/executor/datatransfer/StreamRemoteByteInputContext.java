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
package org.apache.nemo.runtime.executor.datatransfer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.executor.relayserver.RelayServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.*;

import static org.apache.nemo.common.TaskLoc.SF;
import static org.apache.nemo.common.TaskLoc.VM;

/**
 * Container for multiple input streams. Represents a transfer context on receiver-side.
 *
 * <h3>Thread safety:</h3>
 * <p>Methods with default access modifier, namely {@link #onNewStream()}, {@link #onByteBuf(ByteBuf)},
 * {@link #onContextClose()}, are not thread-safe, since they are called by a single Netty event loop.</p>
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public final class StreamRemoteByteInputContext extends AbstractRemoteByteInputContext {

  private static final Logger LOG = LoggerFactory.getLogger(StreamRemoteByteInputContext.class.getName());


  private final RelayServer relayServer;

  private final Channel vmChannel;

  /**
   * Creates an input context.
   * @param remoteExecutorId    id of the remote executor
   * @param contextId           identifier for this context
   * @param contextDescriptor   user-provided context descriptor
   * @param contextManager      {@link ContextManager} for the channel
   */
  public StreamRemoteByteInputContext(final String remoteExecutorId,
                                      final ContextId contextId,
                                      final byte[] contextDescriptor,
                                      final ContextManager contextManager,
                                      final ScheduledExecutorService ackService,
                                      final RelayServer relayServer) {
    super(remoteExecutorId, contextId, contextDescriptor,
      contextManager, ackService, VM, VM);

    this.relayServer = relayServer;
    this.vmChannel  = contextManager.getChannel();
  }

  @Override
  protected ByteTransferContextSetupMessage getStopMessage() {
    final ByteTransferContextSetupMessage pendingMsg =
      new ByteTransferContextSetupMessage(getContextId().getInitiatorExecutorId(),
        getContextId().getTransferIndex(),
        getContextId().getDataDirection(),
        getContextDescriptor(),
        getContextId().isPipe(),
        ByteTransferContextSetupMessage.MessageType.SIGNAL_FROM_CHILD_FOR_STOP_OUTPUT,
        SF,
        taskExecutor.getId(),
        relayServer.getPublicAddress(),
        relayServer.getPort());

    return pendingMsg;
  }

  @Override
  protected void setupInputChannelToParentVM(TaskLoc sendDataTo) {
    final ContextId contextId = getContextId();
    final byte[] contextDescriptor = getContextDescriptor();

    switch (sendDataTo) {
      case VM: {
        // We send ack to the vm channel to initialize it !!!
        final ByteTransferContextSetupMessage settingMsg =
          new ByteTransferContextSetupMessage(contextId.getInitiatorExecutorId(),
            contextId.getTransferIndex(),
            contextId.getDataDirection(),
            contextDescriptor,
            contextId.isPipe(),
            ByteTransferContextSetupMessage.MessageType.SETTING_OUTPUT_CONTEXT,
            VM,
            taskExecutor.getId());

        vmChannel.writeAndFlush(settingMsg);
        break;
      }
    }
  }

  @Override
  protected void sendMessageToRelay(ByteTransferContextSetupMessage msg) {
    throw new UnsupportedOperationException();
  }
}
