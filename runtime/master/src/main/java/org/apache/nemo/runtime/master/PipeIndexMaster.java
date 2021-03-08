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
import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.HashMap;
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

  private final Map<Triple<String, String, String>, Integer> pipeKeyIndexMap = new ConcurrentHashMap<>();
  private final Map<Integer, Triple<String, String, String>> pipeIndexKeyMap = new ConcurrentHashMap<>();
  private final AtomicInteger atomicInteger = new AtomicInteger();

  private final EvalConf evalConf;

  @Inject
  private PipeIndexMaster(final MessageEnvironment masterMessageEnvironment,
                          final EvalConf evalConf) {
    masterMessageEnvironment.setupListener(MessageEnvironment.TASK_INDEX_MESSAGE_LISTENER_ID,
      new TaskIndexMessageReceiver());
    this.evalConf = evalConf;
  }


  public Map<Triple<String, String, String>, Integer> getIndexMapForTask(final String taskId) {
    final Map<Triple<String, String, String>, Integer> m = new HashMap<>();
    pipeKeyIndexMap.forEach((key, val) -> {
      if (key.getLeft().equals(taskId)) {
        m.put(key, val);
      } else if (key.getRight().equals(taskId)) {
        m.put(key, val);
      }
    });
    return m;
  }

  public void onTaskScheduled(final String srcTaskId,
                              final String edgeId,
                              final String dstTaskId) {
    if (!pipeKeyIndexMap.containsKey(Triple.of(srcTaskId, edgeId, dstTaskId))) {
      final int index = atomicInteger.getAndIncrement();

      if (evalConf.controlLogging) {
        LOG.info("Registering pipe {}/{}/{} to {}", srcTaskId, edgeId, dstTaskId, index);
      }
      pipeKeyIndexMap.putIfAbsent(Triple.of(srcTaskId, edgeId, dstTaskId), index);
      pipeIndexKeyMap.putIfAbsent(index, Triple.of(srcTaskId, edgeId, dstTaskId));
    }

    if (!pipeKeyIndexMap.containsKey(Triple.of(dstTaskId, edgeId, srcTaskId))) {
      final int index = atomicInteger.getAndIncrement();

      if (evalConf.controlLogging) {
        LOG.info("Registering pipe {}/{}/{} to {}", dstTaskId, edgeId, srcTaskId, index);
      }
      pipeKeyIndexMap.putIfAbsent(Triple.of(dstTaskId, edgeId, srcTaskId), index);
      pipeIndexKeyMap.putIfAbsent(index, Triple.of(dstTaskId, edgeId, srcTaskId));
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
        case RequestTaskIndex: {
          final ControlMessage.RequestTaskIndexMessage requestTaskIndexMessage =
            message.getRequestTaskIndexMsg();
          final String srcTaskId = requestTaskIndexMessage.getSrcTaskId();
          final String edgeId = requestTaskIndexMessage.getEdgeId();
          final String dstTaskId = requestTaskIndexMessage.getDstTaskId();
          final Triple<String, String, String> key = Triple.of(srcTaskId, edgeId, dstTaskId);

          if (evalConf.controlLogging) {
            LOG.info("Task index of stage: {}: {}", key, pipeKeyIndexMap.get(key));
          }

          if (!pipeKeyIndexMap.containsKey(key)) {
            throw new RuntimeException("No task index for task " + key);
          }

          messageContext.reply(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.TASK_INDEX_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.TaskIndexInfo)
              .setTaskIndexInfoMsg(ControlMessage.TaskIndexInfoMessage.newBuilder()
                .setRequestId(message.getId())
                .setTaskIndex(pipeKeyIndexMap.get(key))
                .build())
              .build());

          break;
        }
        case RequestPipeKey: {
          final ControlMessage.RequestPipeKeyMessage requestPipeKeyMessage =
            message.getRequestPipeKeyMsg();
          final int index = (int)requestPipeKeyMessage.getPipeIndex();

          if (evalConf.controlLogging) {
            LOG.info("Task key of stage: {}", index, pipeIndexKeyMap.get(index));
          }

          if (!pipeIndexKeyMap.containsKey(index)) {
            throw new RuntimeException("No task index for task " + index);
          }

          final Triple<String, String, String> key = pipeIndexKeyMap.get(index);

          messageContext.reply(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.TASK_INDEX_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.RequestPipeKey)
              .setResponsePipeKeyMsg(ControlMessage.ResponsePipeKeyMessage.newBuilder()
                .setSrcTask(key.getLeft())
                .setEdgeId(key.getMiddle())
                .setDstTask(key.getRight())
                .build())
              .build());

          break;
        }
        default:
          throw new IllegalMessageException(new Exception(message.toString()));
      }
    }
  }
}
