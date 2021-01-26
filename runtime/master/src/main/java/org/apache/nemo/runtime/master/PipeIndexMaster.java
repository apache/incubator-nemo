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

import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.common.RuntimeIdManager;
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
public final class PipeIndexMaster {
  private static final Logger LOG = LoggerFactory.getLogger(PipeIndexMaster.class.getName());

  /**
   * Constructor.
   * @param masterMessageEnvironment the message environment.
   */

  private final Map<Triple<String, String, String>, Integer> taskIndexMap = new ConcurrentHashMap<>();
  private final AtomicInteger atomicInteger = new AtomicInteger();

  @Inject
  private PipeIndexMaster(final MessageEnvironment masterMessageEnvironment) {
    masterMessageEnvironment.setupListener(MessageEnvironment.TASK_INDEX_MESSAGE_LISTENER_ID,
      new TaskIndexMessageReceiver());
  }

  public void onTaskScheduled(final String srcTaskId,
                              final String edgeId,
                              final String dstTaskId) {
    if (!taskIndexMap.containsKey(Triple.of(srcTaskId, edgeId, dstTaskId))) {
      taskIndexMap.putIfAbsent(Triple.of(srcTaskId, edgeId, dstTaskId), atomicInteger.getAndIncrement());
    }

    if (!taskIndexMap.containsKey(Triple.of(dstTaskId, edgeId, srcTaskId))) {
      taskIndexMap.putIfAbsent(Triple.of(dstTaskId, edgeId, srcTaskId), atomicInteger.getAndIncrement());
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
          final ControlMessage.RequestTaskIndexMessage requestTaskIndexMessage =
            message.getRequestTaskIndexMsg();
          final String srcTaskId = requestTaskIndexMessage.getSrcTaskId();
          final String edgeId = requestTaskIndexMessage.getEdgeId();
          final String dstTaskId = requestTaskIndexMessage.getDstTaskId();
          final Triple<String, String, String> key = Triple.of(srcTaskId, edgeId, dstTaskId);

          LOG.info("Task index of stage: {}", key, taskIndexMap.get(key));

          if (!taskIndexMap.containsKey(key)) {
            throw new RuntimeException("No task index for task " + key);
          }

          messageContext.reply(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.TASK_INDEX_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.TaskIndexInfo)
              .setTaskIndexInfoMsg(ControlMessage.TaskIndexInfoMessage.newBuilder()
                .setRequestId(message.getId())
                .setTaskIndex(taskIndexMap.get(key))
                .build())
              .build());

          break;
        default:
          throw new IllegalMessageException(new Exception(message.toString()));
      }
    }
  }
}
