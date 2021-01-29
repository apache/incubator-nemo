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
package org.apache.nemo.runtime.executor.data;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.executor.ExecutorContextManagerMap;
import org.apache.nemo.runtime.executor.PipeIndexMapWorker;
import org.apache.nemo.runtime.executor.TaskScheduledMapWorker;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransfer;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskStopSignalByDownstreamTask;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

/**
 * Two threads use this class
 * - Network thread: Saves pipe connections created from destination tasks.
 * - Task executor thread: Creates new pipe connections to destination tasks (read),
 *                         or retrieves a saved pipe connection (write)
 */
@ThreadSafe
public final class PipeManagerWorkerImpl implements PipeManagerWorker {
  private static final Logger LOG = LoggerFactory.getLogger(PipeManagerWorkerImpl.class.getName());

  private final String executorId;

  // To-Executor connections
  private final ByteTransfer byteTransfer;
  // Thread-safe container

  // 여기서 pipe manager worker끼리 N-to-N connection 맺어야.
  private final ExecutorContextManagerMap executorContextManagerMap;
  private final TaskScheduledMapWorker taskScheduledMapWorker;
  private final PipeIndexMapWorker pipeIndexMapWorker;

  // Pipe input
  private final Map<Integer, InputReader> pipeIndexInputReaderMap = new ConcurrentHashMap<>();
  private final Map<String, Set<InputReader>> taskInputReaderMap = new ConcurrentHashMap<>();
  private final Map<InputReader, Set<Integer>> inputReaderPipeIndicesMap = new ConcurrentHashMap<>();

  // Input pipe state
  private final Map<String, InputPipeState> taskInputPipeState = new ConcurrentHashMap<>();


  // For pipe stop restart
  private final Map<Integer, List<Object>> pendingOutputPipeMap = new ConcurrentHashMap<>();
  private final Map<String, Set<Integer>> pendingTaskPipeIndicesMap = new ConcurrentHashMap<>();

  //          edge1                                               edge2
  //  T1  --(index)-->  [InputReader (T4, edge1)]  --> <T4> --> [OutputWriter]  --(index)-->   T5
  //  T2  --(index)-->                                                        --(index)-->   T6
  //  T3  --(index)-->                                                        --(index)-->   T7
  @Inject
  private PipeManagerWorkerImpl(@Parameter(JobConf.ExecutorId.class) final String executorId,
                                final ByteTransfer byteTransfer,
                                final ExecutorContextManagerMap executorContextManagerMap,
                                final TaskScheduledMapWorker taskScheduledMapWorker,
                                final PipeIndexMapWorker pipeIndexMapWorker) {
    this.executorId = executorId;
    this.byteTransfer = byteTransfer;
    this.executorContextManagerMap = executorContextManagerMap;
    this.taskScheduledMapWorker = taskScheduledMapWorker;
    this.pipeIndexMapWorker = pipeIndexMapWorker;
  }

  @Override
  public void registerInputPipe(final String srcTaskId,
                                final String edgeId,
                                final String dstTaskId,
                                final InputReader reader) {
    // taskId로부터 받는 data를 위한 input reader
    final int pipeIndex = pipeIndexMapWorker.getPipeIndex(srcTaskId, edgeId, dstTaskId);

    LOG.info("Registering pipe for input reader {}/{}/{} . index: {}",
      srcTaskId, edgeId, dstTaskId, pipeIndex);

    taskInputReaderMap.putIfAbsent(dstTaskId, new HashSet<>());
    taskInputReaderMap.get(dstTaskId).add(reader);

    taskInputPipeState.putIfAbsent(dstTaskId, InputPipeState.RUNNING);

    inputReaderPipeIndicesMap.putIfAbsent(reader, new HashSet<>());
    inputReaderPipeIndicesMap.get(reader).add(pipeIndex);

    if (pipeIndexInputReaderMap.containsKey(pipeIndex)) {
      throw new RuntimeException("Pipe is already registered " + pipeIndex);
    }
    pipeIndexInputReaderMap.put(pipeIndex, reader);
  }

  private final Map<String, List<Integer>> inputStopSignalPipes = new ConcurrentHashMap<>();
  @Override
  public void sendSignalForPipes(final List<String> srcTasks,
                                 final String edgeId,
                                 final String dstTaskId,
                                 final Signal signal) {
    inputStopSignalPipes.putIfAbsent(dstTaskId, new ArrayList<>(srcTasks.size()));
    for (final String srcTask : srcTasks) {
      final int pipeIndex = pipeIndexMapWorker.getPipeIndex(srcTask, edgeId, dstTaskId);
      inputStopSignalPipes.get(dstTaskId).add(pipeIndex);
    }

    for (final String srcTask : srcTasks) {
      final ContextManager contextManager = getContextManagerForDstTask(srcTask);
      final Channel channel = contextManager.getChannel();
      final int pipeIndex = pipeIndexMapWorker.getPipeIndex(dstTaskId, edgeId, srcTask);
      final int targetPipeIndex = pipeIndexMapWorker.getPipeIndex(srcTask, edgeId, dstTaskId);

      TaskControlMessage controlMessage = null;
      switch (signal) {
        case INPUT_STOP: {
          taskInputPipeState.put(dstTaskId, InputPipeState.WAITING_ACK);
          controlMessage = new TaskControlMessage(
            TaskControlMessage.TaskControlMessageType.PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK,
            pipeIndex,
            targetPipeIndex,
            srcTask,
            new TaskStopSignalByDownstreamTask(dstTaskId, edgeId, srcTask));
          break;
        }
        case INPUT_RESTART: {
          break;
        }
        default: {
          throw new RuntimeException("Invalid type " + signal);
        }
      }

      channel.writeAndFlush(controlMessage)
        .addListener(listener);
    }
  }

  @Override
  public void receiveAckInputStopSignal(String taskId, int pipeIndex) {

    if (!inputStopSignalPipes.containsKey(taskId)) {
      throw new RuntimeException("Invalid pipe stop ack " + taskId + ", " + pipeIndex);
    }

    final List<Integer> stopPipes = inputStopSignalPipes.get(taskId);
    synchronized (stopPipes) {
      stopPipes.remove((Integer) pipeIndex);

      if (stopPipes.isEmpty()) {
        inputStopSignalPipes.remove(taskId);
        taskInputPipeState.put(taskId, InputPipeState.STOPPED);
      }
    }
  }

  @Override
  public InputPipeState getInputPipeState(String taskId) {
    return taskInputPipeState.get(taskId);
  }

  @Override
  public void broadcast(final String srcTaskId,
                        final String edgeId,
                        List<String> dstTasks, Serializer serializer, Object event) {
    // LOG.info("Broadcast watermark in pipeline Manager worker {}", event);
    final Map<String, List<Integer>> executorDstTaskIndicesMap = new HashMap<>();

    dstTasks.forEach(dstTask -> {
      final String executorId = taskScheduledMapWorker.getRemoteExecutorId(dstTask);
      executorDstTaskIndicesMap.putIfAbsent(executorId, new LinkedList<>());
      executorDstTaskIndicesMap.get(executorId).add(
      pipeIndexMapWorker.getPipeIndex(srcTaskId, edgeId, dstTask));
    });

    for (final String remoteExecutorId : executorDstTaskIndicesMap.keySet()) {
      final ContextManager contextManager = byteTransfer.getRemoteExecutorContetxManager(remoteExecutorId);
      final Channel channel = contextManager.getChannel();

      final ByteBuf byteBuf = channel.alloc().ioBuffer();
      final ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(byteBuf);

      try {
        final OutputStream wrapped = byteBufOutputStream;
        //DataUtil.buildOutputStream(byteBufOutputStream, serializer.getEncodeStreamChainers());
        final EncoderFactory.Encoder encoder = serializer.getEncoderFactory().create(wrapped);
        //LOG.info("Element encoder: {}", encoder);
        encoder.encode(event);
        wrapped.close();
      } catch (final IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      // pending pipe checks
      final List<Integer> pipeIndices = executorDstTaskIndicesMap.get(remoteExecutorId);
      List<Integer> pendingPipes = null;
      final Iterator<Integer> iterator = pipeIndices.iterator();
      while (iterator.hasNext()) {
        final int index = iterator.next();
        if (pendingOutputPipeMap.containsKey(index)) {
          if (pendingPipes == null) {
            pendingPipes = new LinkedList<>();
          }
          pendingPipes.add(index);
          iterator.remove();
        }
      }

      if (pendingPipes != null) {
        pendingPipes.forEach(pendingIndex -> {
          pendingOutputPipeMap.get(pendingIndex).add(
          DataFrameEncoder.DataFrame.newInstance(
            Collections.singletonList(pendingIndex), byteBuf.retainedDuplicate(), byteBuf.readableBytes(), true));
        });
      }

      channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(
        pipeIndices, byteBuf, byteBuf.readableBytes(), true))
        .addListener(listener);
    }
  }

  private ContextManager getContextManagerForDstTask(final String dstTaskId) {
    return executorContextManagerMap
      .getExecutorContextManager(
      taskScheduledMapWorker.getRemoteExecutorId(dstTaskId));
  }

  @Override
  public void writeData(final String srcTaskId,
                        final String edgeId,
                        final String dstTaskId,
                        final Serializer serializer,
                        final Object event) {
    final int index = pipeIndexMapWorker.getPipeIndex(srcTaskId, edgeId, dstTaskId);

    Stream.of(pendingOutputPipeMap.containsKey(index))
      .map(outputStopped -> {
        if (outputStopped) {
          final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.ioBuffer();
          return Triple.of(outputStopped, new ByteBufOutputStream(byteBuf), (Channel) null);
        } else {
          final ContextManager contextManager = getContextManagerForDstTask(dstTaskId);
          final Channel channel = contextManager.getChannel();
          final ByteBuf byteBuf = channel.alloc().ioBuffer();
          return Triple.of(outputStopped, new ByteBufOutputStream(byteBuf), channel);
        }
      })
      .forEach(triple -> {
        final ByteBufOutputStream byteBufOutputStream = triple.getMiddle();
        try {
          final OutputStream wrapped = byteBufOutputStream;
          final EncoderFactory.Encoder encoder = serializer.getEncoderFactory().create(wrapped);
          encoder.encode(event);
          wrapped.close();
        } catch (final IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }

        final ByteBuf byteBuf = byteBufOutputStream.buffer();

        final Object finalData = DataFrameEncoder.DataFrame.newInstance(
          Collections.singletonList(index), byteBuf, byteBuf.readableBytes(), true);

        if (triple.getLeft()) {
          // this is pending output pipe
          pendingOutputPipeMap.get(index).add(finalData);
        } else {
          triple.getRight().writeAndFlush(finalData)
            .addListener(listener);
        }
      });
  }

  @Override
  public void addInputData(final int index, ByteBuf event) {
    if (!pipeIndexInputReaderMap.containsKey(index)) {
      throw new RuntimeException("Invalid task index " + index);
    }
    pipeIndexInputReaderMap.get(index).addData(index, event);
  }

  @Override
  public void addControlData(int index, TaskControlMessage controlMessage) {
     if (!pipeIndexInputReaderMap.containsKey(index)) {
       throw new RuntimeException("Invalid task index " + index);
     }
     pipeIndexInputReaderMap.get(index).addControl(controlMessage);
  }

  @Override
  public void stopOutputPipe(int index, String taskId) {
    if (pendingOutputPipeMap.containsKey(index)) {
      throw new RuntimeException("Output pipe already stopped " + index + " " + taskId);
    }

    if (taskInputPipeState.get(taskId).equals(InputPipeState.STOPPED)) {
      LOG.info("Task is already removed " + taskId);
    } else {
      pendingOutputPipeMap.putIfAbsent(index, new LinkedList<>());
      pendingTaskPipeIndicesMap.putIfAbsent(taskId, new HashSet<>());

      synchronized (pendingTaskPipeIndicesMap.get(taskId)) {
        pendingTaskPipeIndicesMap.get(taskId).add(index);
      }
    }

    // send control message
    final Triple<String, String, String> key = pipeIndexMapWorker.getKey(index);
    final ContextManager contextManager = getContextManagerForDstTask(key.getRight());
    final Channel channel = contextManager.getChannel();


    channel.writeAndFlush(new TaskControlMessage(
      TaskControlMessage.TaskControlMessageType.PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK,
      index,
      index,
      key.getRight(),
      null));
  }

  @Override
  public void restartOutputPipe(int index, String taskId) {
    // restart pending output
    if (pendingOutputPipeMap.containsKey(index)) {
      LOG.info("Restart pipe " + index);
      final Triple<String, String, String> key = pipeIndexMapWorker.getKey(index);
      final ContextManager contextManager = getContextManagerForDstTask(key.getRight());
      final Channel channel = contextManager.getChannel();

      final List<Object> pendingData = pendingOutputPipeMap.remove(index);
      pendingData.forEach(data -> channel.write(data));

      channel.flush();

      pendingTaskPipeIndicesMap.get(taskId).remove((Integer) index);
      if (pendingTaskPipeIndicesMap.get(taskId).isEmpty()) {
        pendingTaskPipeIndicesMap.remove(taskId);
      }
    } else {
      LOG.info("Start pipe " + index);
    }
  }

  @Override
  public boolean isOutputPipeStopped(String taskId) {
    return pendingTaskPipeIndicesMap.containsKey(taskId);
  }

  @Override
  public void flush() {
    executorContextManagerMap.getExecutorContextManagers().forEach(contextManager -> {
      contextManager.getChannel().flush();
    });
  }

  @Override
  public <T> CompletableFuture<T> request(int taskIndex, Object event) {
    throw new RuntimeException("Not implemented yet");
  }

  @Override
  public void close() {
    executorContextManagerMap.getExecutorContextManagers().forEach(contextManager -> {
      contextManager.getChannel().close();
    });
  }

  private final GenericFutureListener listener = new GenericFutureListener<Future<? super Void>>() {
    @Override
    public void operationComplete(Future<? super Void> future) throws Exception {
      if (future.isSuccess()) {
        return;
      } else {
        LOG.warn(future.cause().getMessage());
        try {
          throw future.cause();
        } catch (Throwable throwable) {
          throwable.printStackTrace();
          new RuntimeException(throwable);
        }
      }
    }
  };
}
