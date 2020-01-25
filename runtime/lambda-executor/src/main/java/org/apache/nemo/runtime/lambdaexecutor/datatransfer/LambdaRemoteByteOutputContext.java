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
package org.apache.nemo.runtime.lambdaexecutor.datatransfer;

import io.netty.channel.Channel;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.common.TaskLocationMap;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.nemo.common.TaskLoc.SF;
import static org.apache.nemo.common.TaskLoc.VM;

/**
 * Container for multiple output streams. Represents a transfer context on sender-side.
 *
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public final class LambdaRemoteByteOutputContext extends AbstractRemoteByteOutputContext {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaRemoteByteOutputContext.class.getName());

  private final RelayServerClient relayServerClient;
  private final TaskLoc myLocation;

  /**
   * Creates a output context.
   *
   * @param remoteExecutorId    id of the remote executor
   * @param contextId           identifier for this context
   * @param contextDescriptor   user-provided context descriptor
   * @param contextManager      {@link ContextManager} for the channel
   */
  public LambdaRemoteByteOutputContext(final String remoteExecutorId,
                                       final ContextId contextId,
                                       final byte[] contextDescriptor,
                                       final ContextManager contextManager,
                                       final String relayDst,
                                       final TaskLoc sendDataTo,
                                       final RelayServerClient relayServerClient,
                                       final TaskLoc myLocation,
                                       final TaskLocationMap taskLocationMap) {
    super(remoteExecutorId, contextId, contextDescriptor,
      contextManager, myLocation, sendDataTo, relayDst, taskLocationMap);
    //LOG.info("RelayDst {} for remoteExecutor: {}, channel {}", relayDst, remoteExecutorId,
    //  relayChannel);
    this.relayServerClient = relayServerClient;
    this.myLocation = myLocation;
  }

  @Override
  protected void setupOutputChannelToParentVM(ByteTransferContextSetupMessage msg, TaskLoc sendDataTo) {
    final LambdaContextManager cm = (LambdaContextManager) contextManager;
    final PipeTransferContextDescriptor cd = PipeTransferContextDescriptor.decode(getContextDescriptor());

    if (myLocation.equals(SF) && sendDataTo.equals(SF)) {
      final String relayServerAddress = msg.getRelayServerAddress();
      final int relayServerPort = msg.getRelayServerPort();
      //LOG.info("Connecting to relay server for input {}/{}, transferIndex: {}",
      //  relayServerAddress, relayServerPort, transferIndex);

      cm.connectToRelay(relayServerAddress, relayServerPort, (relayServerChannel) -> {
        // 내 채널에  destination 등록!!
        final Channel myRelayServer = relayServerClient.getRelayServerChannel(getLocalExecutorId());
        //LOG.info("Connect to my relay server for child stop {}/{}", localExecutorId, myRelayServer);
        LOG.info("Send init message for the connected relay server for scaling in... {} to {}", taskId, msg.getTaskId());
        relayServerClient.registerTask(myRelayServer, cd.getRuntimeEdgeId(), (int) cd.getSrcTaskIndex(), false);
      });
    } else if (sendDataTo.equals(VM)) {
      cm.connectToVm(msg.getInitiatorExecutorId(), (vmContextManager) -> {
        // We send ack to the vm channel to initialize it !!!
        final ByteTransferContextSetupMessage ackMessage =
          new ByteTransferContextSetupMessage(getContextId().getInitiatorExecutorId(),
            getContextId().getTransferIndex(),
            getContextId().getDataDirection(),
            getContextDescriptor(),
            getContextId().isPipe(),
            ByteTransferContextSetupMessage.MessageType.SETTING_INPUT_CONTEXT,
            myLocation,
            taskId);
        LOG.info("Send init message for the connected VM for scaling in... {} to {}", taskId, msg.getTaskId());
        vmContextManager.getChannel().write(ackMessage);
      });
    }

  }
}
