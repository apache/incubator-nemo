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
package org.apache.nemo.runtime.master;

import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Master-side pipe manager.
 */
@ThreadSafe
@DriverSide
public final class TransferIndexMaster {
  private static final Logger LOG = LoggerFactory.getLogger(TransferIndexMaster.class.getName());

  private final AtomicInteger inputContextIndex;
  private final AtomicInteger outputContextIndex;

  /**
   * Constructor.
   * @param masterMessageEnvironment the message environment.
   */
  @Inject
  private TransferIndexMaster(final MessageEnvironment masterMessageEnvironment) {
    masterMessageEnvironment.setupListener(MessageEnvironment.TRANSFER_INDEX_LISTENER_ID,
      new TransferIndexReceiver());

    this.inputContextIndex = new AtomicInteger();
    this.outputContextIndex = new AtomicInteger();
  }

  /**
   * Handler for control messages received.
   */
  public final class TransferIndexReceiver implements MessageListener<ControlMessage.Message> {
    @Override
    public void onMessage(final ControlMessage.Message message) {
      throw new RuntimeException("Exception " + message);
    }

    @Override
    public void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
        case RequestTransferIndex:
          final ControlMessage.RequestTransferIndexMessage requestIndexMessage = message.getRequestTransferIndexMsg();
          final int isInputContext = (int) requestIndexMessage.getIsInputContext();

          final int index = isInputContext == 1 ? inputContextIndex.getAndIncrement() : outputContextIndex.getAndIncrement();

          LOG.info("Send input/output ({}) context index {}", isInputContext, index);

          messageContext.reply(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.TRANSFER_INDEX_LISTENER_ID)
              .setType(ControlMessage.MessageType.TransferIndexInfo)
              .setTransferIndexInfoMsg(ControlMessage.TransferIndexInfoMessage.newBuilder()
                .setRequestId(message.getId())
                .setIndex(index)
                .build())
              .build());

          break;
        default:
          throw new IllegalMessageException(new Exception(message.toString()));
      }
    }
  }
}
