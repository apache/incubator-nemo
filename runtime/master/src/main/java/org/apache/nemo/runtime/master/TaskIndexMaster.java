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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Master-side pipe manager.
 */
@ThreadSafe
@DriverSide
public final class TaskIndexMaster {
  private static final Logger LOG = LoggerFactory.getLogger(TaskIndexMaster.class.getName());
  private final Map<String, AtomicInteger> stageIndexMap;

  /**
   * Constructor.
   * @param masterMessageEnvironment the message environment.
   */
  @Inject
  private TaskIndexMaster(final MessageEnvironment masterMessageEnvironment) {
    masterMessageEnvironment.setupListener(MessageEnvironment.SCALEOUT_MESSAGE_LISTENER_ID,
      new TaskIndexMessageReceiver());

    this.stageIndexMap = new ConcurrentHashMap<>();
  }

  public void onTaskScheduled(final String taskId) {
    final String stageId  = RuntimeIdManager.getStageIdFromTaskId(taskId);
    if (stageIndexMap.putIfAbsent(stageId, new AtomicInteger(1)) != null) {
      stageIndexMap.get(stageId).getAndIncrement();
    }
  }

  /**
   * Handler for control messages received.
   */
  public final class TaskIndexMessageReceiver implements MessageListener<ControlMessage.Message> {
    @Override
    public void onMessage(final ControlMessage.Message message) {
      throw new RuntimeException("Exception " + message);
    }

    @Override
    public void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
        case RequestTaskIndex:
          final ControlMessage.RequestTaskIndexMessage requestTaskIndexMessage = message.getRequestTaskIndexMsg();
          final String stageId = RuntimeIdManager.getStageIdFromTaskId(requestTaskIndexMessage.getTaskId());
          final int index = stageIndexMap.get(stageId).getAndIncrement();

          LOG.info("Task index of stage: {}, {}", stageId, index);

          messageContext.reply(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.SCALEOUT_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.TaskIndexInfo)
              .setTaskIndexInfoMsg(ControlMessage.TaskIndexInfoMessage.newBuilder()
                .setRequestId(message.getId())
                .setTaskIndex(index)
                .build())
              .build());

          break;
        default:
          throw new IllegalMessageException(new Exception(message.toString()));
      }
    }
  }
}
