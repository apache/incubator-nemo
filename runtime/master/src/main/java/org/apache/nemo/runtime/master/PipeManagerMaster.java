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
package org.apache.nemo.runtime.master;

import org.apache.nemo.common.Pair;
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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Master-side pipe manager.
 */
@ThreadSafe
@DriverSide
public final class PipeManagerMaster {
  private static final Logger LOG = LoggerFactory.getLogger(PipeManagerMaster.class.getName());
  private final Map<Pair<String, Long>, String> runtimeEdgeSrcIndexToExecutor;
  private final Map<Pair<String, Long>, Lock> runtimeEdgeSrcIndexToLock;
  private final Map<Pair<String, Long>, Condition> runtimeEdgeSrcIndexToCondition;
  private final ExecutorService waitForPipe;

  /**
   * Constructor.
   * @param masterMessageEnvironment the message environment.
   */
  @Inject
  private PipeManagerMaster(final MessageEnvironment masterMessageEnvironment) {
    masterMessageEnvironment.setupListener(MessageEnvironment.PIPE_MANAGER_MASTER_MESSAGE_LISTENER_ID,
      new PipeManagerMasterControlMessageReceiver());
    this.runtimeEdgeSrcIndexToExecutor = new ConcurrentHashMap<>();
    this.runtimeEdgeSrcIndexToLock = new ConcurrentHashMap<>();
    this.runtimeEdgeSrcIndexToCondition = new ConcurrentHashMap<>();
    this.waitForPipe = Executors.newCachedThreadPool();
  }

  public void onTaskScheduled(final String edgeId, final long srcIndex) {
    final Pair<String, Long> keyPair = Pair.of(edgeId, srcIndex);
    if (null != runtimeEdgeSrcIndexToLock.put(keyPair, new ReentrantLock())) {
      throw new IllegalStateException(keyPair.toString());
    }
    if (null != runtimeEdgeSrcIndexToCondition.put(keyPair, runtimeEdgeSrcIndexToLock.get(keyPair).newCondition())) {
      throw new IllegalStateException(keyPair.toString());
    }
  }

  /**
   * Handler for control messages received.
   */
  public final class PipeManagerMasterControlMessageReceiver implements MessageListener<ControlMessage.Message> {
    @Override
    public void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
        case PipeInit:
          final ControlMessage.PipeInitMessage pipeInitMessage = message.getPipeInitMsg();
          final Pair<String, Long> keyPair =
            Pair.of(pipeInitMessage.getRuntimeEdgeId(), pipeInitMessage.getSrcTaskIndex());

          // Allow to put at most once
          final Lock lock = runtimeEdgeSrcIndexToLock.get(keyPair);
          lock.lock();
          try {
            if (null != runtimeEdgeSrcIndexToExecutor.put(keyPair, pipeInitMessage.getExecutorId())) {
              throw new RuntimeException(keyPair.toString());
            }
            runtimeEdgeSrcIndexToCondition.get(keyPair).signalAll();
          } finally {
            lock.unlock();
          }

          break;
        default:
          throw new IllegalMessageException(new Exception(message.toString()));
      }


    }

    @Override
    public void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
        case RequestPipeLoc:
          final ControlMessage.RequestPipeLocationMessage pipeLocRequest = message.getRequestPipeLocMsg();

          // Use the executor service to avoid blocking the networking thread.
          waitForPipe.submit(() -> {
            final Pair<String, Long> keyPair =
              Pair.of(pipeLocRequest.getRuntimeEdgeId(), pipeLocRequest.getSrcTaskIndex());

            final Lock lock = runtimeEdgeSrcIndexToLock.get(keyPair);
            lock.lock();
            try {
              if (!runtimeEdgeSrcIndexToExecutor.containsKey(keyPair)) {
                runtimeEdgeSrcIndexToCondition.get(keyPair).await();
              }

              final String location = runtimeEdgeSrcIndexToExecutor.get(keyPair);
              if (location == null) {
                throw new IllegalStateException(keyPair.toString());
              }

              // Reply the location
              messageContext.reply(
                ControlMessage.Message.newBuilder()
                  .setId(RuntimeIdManager.generateMessageId())
                  .setListenerId(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID)
                  .setType(ControlMessage.MessageType.PipeLocInfo)
                  .setPipeLocInfoMsg(ControlMessage.PipeLocationInfoMessage.newBuilder()
                    .setRequestId(message.getId())
                    .setExecutorId(location)
                    .build())
                  .build());
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            } finally {
              lock.unlock();
            }
          });

          break;
        default:
          throw new IllegalMessageException(new Exception(message.toString()));
      }
    }
  }
}
