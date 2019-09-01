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

import io.netty.channel.Channel;
import org.apache.nemo.common.TaskLoc;

import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.nemo.common.TaskLoc.VM;

/**
 * Container for multiple output streams. Represents a transfer context on sender-side.
 *
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public final class RemoteByteOutputContext extends AbstractRemoteByteOutputContext {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteByteOutputContext.class.getName());

  private final Channel vmChannel;

  /**
   * Creates a output context.
   *
   * @param remoteExecutorId    id of the remote executor
   * @param contextId           identifier for this context
   * @param contextDescriptor   user-provided context descriptor
   * @param contextManager      {@link ContextManager} for the channel
   */
  public RemoteByteOutputContext(final String remoteExecutorId,
                                 final ContextId contextId,
                                 final byte[] contextDescriptor,
                                 final ContextManager contextManager) {
    super(remoteExecutorId, contextId, contextDescriptor, contextManager, VM, VM, "");
    this.vmChannel = contextManager.getChannel();
  }

  @Override
  protected void setupOutputChannelToParentVM(ByteTransferContextSetupMessage msg, TaskLoc sendDataTo) {

    if (sendDataTo.equals(VM)) {
      // sf -> vm
      // send ack to the vm channel
      //LOG.info("Sending ack from parent stop output to ");
      final ByteTransferContextSetupMessage ackMessage =
        new ByteTransferContextSetupMessage(getContextId().getInitiatorExecutorId(),
          getContextId().getTransferIndex(),
          getContextId().getDataDirection(),
          getContextDescriptor(),
          getContextId().isPipe(),
          ByteTransferContextSetupMessage.MessageType.SETTING_INPUT_CONTEXT,
          VM,
          taskId);

      vmChannel.writeAndFlush(ackMessage);
    }
  }
}
