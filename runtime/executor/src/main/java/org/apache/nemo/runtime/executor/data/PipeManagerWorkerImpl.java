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
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.executor.ExecutorChannelManagerMap;
import org.apache.nemo.runtime.executor.PipeIndexMapWorker;
import org.apache.nemo.runtime.executor.TaskExecutorMapWrapper;
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
import java.util.stream.Collectors;

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
  private final ExecutorChannelManagerMap executorChannelManagerMap;
  private final TaskScheduledMapWorker taskScheduledMapWorker;
  private final PipeIndexMapWorker pipeIndexMapWorker;
  private final TaskExecutorMapWrapper taskExecutorMapWrapper;

  // Pipe input
  private final Map<Integer, InputReader> inputPipeIndexInputReaderMap = new ConcurrentHashMap<>();
  // private final Map<String, Set<InputReader>> taskInputReaderMap = new ConcurrentHashMap<>();
  // private final Map<InputReader, Set<Integer>> inputReaderPipeIndicesMap = new ConcurrentHashMap<>();

  // Input pipe state
  private final Map<String, InputPipeState> taskInputPipeState = new ConcurrentHashMap<>();


  // For pipe stop restart
  private final Map<Integer, List<Object>> pendingOutputPipeMap = new ConcurrentHashMap<>();
  private final Map<String, Set<Integer>> taskStoppedOutputPipeIndicesMap = new ConcurrentHashMap<>();

  private final Map<String, Set<Integer>> pipeOuptutIndicesForDstTask = new ConcurrentHashMap<>();

  private final EvalConf evalConf;

  //          edge1                                               edge2
  //  T1  --(index)-->  [InputReader (T4, edge1)]  --> <T4> --> [OutputWriter]  --(index)-->   T5
  //  T2  --(index)-->                                                        --(index)-->   T6
  //  T3  --(index)-->                                                        --(index)-->   T7
  @Inject
  private PipeManagerWorkerImpl(@Parameter(JobConf.ExecutorId.class) final String executorId,
                                final ByteTransfer byteTransfer,
                                final EvalConf evalConf,
                                final ExecutorChannelManagerMap executorChannelManagerMap,
                                final TaskScheduledMapWorker taskScheduledMapWorker,
                                final PipeIndexMapWorker pipeIndexMapWorker,
                                final TaskExecutorMapWrapper taskExecutorMapWrapper) {
    this.executorId = executorId;
    this.evalConf = evalConf;
    this.byteTransfer = byteTransfer;
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    this.executorChannelManagerMap = executorChannelManagerMap;
    this.taskScheduledMapWorker = taskScheduledMapWorker;
    this.pipeIndexMapWorker = pipeIndexMapWorker;
    LOG.info("PipeManagerWorkImpl instance {} in executor {}", hashCode(), executorId);
  }

  @Override
  public void retrieveIndexForOffloadingSource(String srcTaskId, String edgeId) {
    final int inputPipeIndex = pipeIndexMapWorker.getPipeIndex("Origin", edgeId, srcTaskId);
    LOG.info("Retrieve index for prepareOffloading {} / {}", srcTaskId, inputPipeIndex);
  }

  @Override
  public synchronized void registerInputPipe(final String srcTaskId,
                                final String edgeId,
                                final String dstTaskId,
                                final InputReader reader) {
    // taskId로부터 받는 data를 위한 input reader
    final int inputPipeIndex = pipeIndexMapWorker.getPipeIndex(srcTaskId, edgeId, dstTaskId);


    // taskInputReaderMap.putIfAbsent(dstTaskId, new HashSet<>());
    // taskInputReaderMap.get(dstTaskId).add(reader);
    // inputReaderPipeIndicesMap.putIfAbsent(reader, new HashSet<>());
    // inputReaderPipeIndicesMap.get(reader).add(inputPipeIndex);

    try {
      taskInputPipeState.put(dstTaskId, InputPipeState.RUNNING);

      if (evalConf.controlLogging) {
        LOG.info("Registering input pipe index {}/{}/{} . index: {}, state {}",
          srcTaskId, edgeId, dstTaskId, inputPipeIndex, taskInputPipeState.get(dstTaskId));
      }

      if (inputPipeIndexInputReaderMap.containsKey(inputPipeIndex)) {
        LOG.warn("Pipe was already registered.. because the task was running in this executor {}/{}/{}  index {} in executor {} ",
          srcTaskId, edgeId, dstTaskId, inputPipeIndex, executorId);
      }

      inputPipeIndexInputReaderMap.put(inputPipeIndex, reader);

      // Send init message
      // If the dst task is not scheduled yet
      // We should buffer the message and flush when the dst task is scheduled and pipe is initiated
      final int outputPipeIndex = pipeIndexMapWorker.getPipeIndex(dstTaskId, edgeId, srcTaskId);

      // A -> B [*] inputPipeIndex of B == outputPipeIndex of A
      // B -> A  (outputPipeIndex) of B  == (inputPIpeInex) of A
      final TaskControlMessage controlMessage = new TaskControlMessage(
        TaskControlMessage.TaskControlMessageType.PIPE_INIT,
        outputPipeIndex, // A's input pipe index
        inputPipeIndex, // A's output pipe index
        srcTaskId,
        null);

      final Optional<Channel> optional = getChannelForDstTask(srcTaskId, true);
      if (optional.isPresent()) {
        optional.get().writeAndFlush(controlMessage);
      } else {
        // this is pending output pipe
        // the task is not sheduled yet
        if (evalConf.controlLogging) {
          LOG.info("Output pipe {}/{}/{} is not registered yet.. waiting for schedule {} in executor {}",
            dstTaskId, edgeId, srcTaskId, srcTaskId, executorId);
        }
        synchronized (pendingOutputPipeMap) {
          pendingOutputPipeMap.putIfAbsent(outputPipeIndex, new LinkedList<>());
          pendingOutputPipeMap.get(outputPipeIndex).add(controlMessage);
        }
        pipeOuptutIndicesForDstTask.putIfAbsent(srcTaskId, new HashSet<>());
        pipeOuptutIndicesForDstTask.get(srcTaskId).add(outputPipeIndex);
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private final Map<String, List<Integer>> inputStopSignalPipes = new ConcurrentHashMap<>();
  @Override
  public synchronized void sendStopSignalForInputPipes(final List<String> srcTasks,
                                          final String edgeId,
                                          final String dstTaskId) {
    inputStopSignalPipes.putIfAbsent(dstTaskId, new ArrayList<>(srcTasks.size()));
    for (final String srcTask : srcTasks) {
      final int pipeIndex = pipeIndexMapWorker.getPipeIndex(srcTask, edgeId, dstTaskId);
      inputStopSignalPipes.get(dstTaskId).add(pipeIndex);
    }

    for (final String srcTask : srcTasks) {
      final Optional<Channel> optional = getChannelForDstTask(srcTask, true);
      if (!optional.isPresent()) {
        LOG.warn("Task {} is already removed... dont have to wait for the ack in executor {}", srcTask, executorId);
        final int pipeIndex = pipeIndexMapWorker.getPipeIndex(srcTask, edgeId, dstTaskId);
        inputStopSignalPipes.get(dstTaskId).remove((Integer) pipeIndex);
      } else {
        final Channel channel = optional.get();
        final int myOutputPipeIndex = pipeIndexMapWorker.getPipeIndex(dstTaskId, edgeId, srcTask);
        final int myInputPipeIndex = pipeIndexMapWorker.getPipeIndex(srcTask, edgeId, dstTaskId);

        if (evalConf.controlLogging) {
          LOG.info("Send stop signal for input pipes {} index {} in executor {}", dstTaskId, myInputPipeIndex, executorId);
        }

        taskInputPipeState.put(dstTaskId, InputPipeState.WAITING_ACK);
        final TaskControlMessage controlMessage = new TaskControlMessage(
          TaskControlMessage.TaskControlMessageType.PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK,
          myOutputPipeIndex,
          myInputPipeIndex,
          srcTask,
          new TaskStopSignalByDownstreamTask(dstTaskId, edgeId, srcTask));

        channel.writeAndFlush(controlMessage)
          .addListener(listener);
      }
    }
  }

  @Override
  public void setTaskStop(String taskId) {
    taskInputPipeState.put(taskId, InputPipeState.STOPPED);
  }

  @Override
  public void receiveAckInputStopSignal(String taskId, int pipeIndex) {

    if (!inputStopSignalPipes.containsKey(taskId)) {
      throw new RuntimeException("Invalid pipe stop ack " + taskId + ", " + pipeIndex);
    }

    if (evalConf.controlLogging) {
      LOG.info("Receive act input stop signal for {} index {} in executor {}", taskId, pipeIndex, executorId);
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
  public synchronized InputPipeState getInputPipeState(String taskId) {
    return taskInputPipeState.get(taskId);
  }

  @Override
  public synchronized boolean isInputPipeStopped(String taskId) {
    if (evalConf.controlLogging) {
      LOG.info("TaskInputPipeState {} in executor {}, requesting {}", taskInputPipeState, executorId, taskId);
    }
    return taskInputPipeState.get(taskId).equals(InputPipeState.STOPPED);
  }

  final class BroadcastPending {
    public final String edgeId;
    public final List<String> dstTasks;
    public final Serializer serializer;
    public final Object event;

    public BroadcastPending(final String edgeId,
                            final List<String> dstTasks,
                            final Serializer serializer,
                            final Object event) {
      this.edgeId = edgeId;
      this.dstTasks = dstTasks;
      this.serializer = serializer;
      this.event = event;
    }
  }


  private void broadcastInternal(final String srcTaskId,
                                final String edgeId,
                                List<String> dstTasks, Serializer serializer, Object event) {
    final Map<String, List<Integer>> executorDstTaskIndicesMap = new HashMap<>();

    dstTasks.forEach(dstTask -> {
      final String executorId = taskScheduledMapWorker.getRemoteExecutorId(dstTask, false);
      if (executorId == null || executorId.equals("null")) {
        throw new RuntimeException("executor id is null for task " + dstTask);
      }
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
          synchronized (pendingOutputPipeMap) {
            if (pendingOutputPipeMap.containsKey(pendingIndex)) {
              pendingOutputPipeMap.get(pendingIndex).add(
                DataFrameEncoder.DataFrame.newInstance(
                  pendingIndex, byteBuf.retainedDuplicate(), byteBuf.readableBytes(), true));
            } else {
              pipeIndices.add(pendingIndex);
            }
          }
        });
      }

      if (pipeIndices.size() > 0) {
        channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(
          pipeIndices, byteBuf, byteBuf.readableBytes(), true))
          .addListener(listener);
      }
    }
  }

  private final Map<String, List<BroadcastPending>> broadcastPendingMap = new ConcurrentHashMap<>();
  private void flushNoChannelBroadcast(final String srcTaskId) {
    if (broadcastPendingMap.containsKey(srcTaskId)) {
      final List<BroadcastPending> list = broadcastPendingMap.get(srcTaskId);
      final Iterator<BroadcastPending> iterator = list.iterator();
      while (iterator.hasNext()) {
        final BroadcastPending pending = iterator.next();
        if (pending.dstTasks.stream()
          .filter(dstTaskId -> !getChannelForDstTask(dstTaskId, false).isPresent())
          .count() == 0) {
          iterator.remove();
          broadcastInternal(srcTaskId, pending.edgeId, pending.dstTasks, pending.serializer, pending.event);
          LOG.info("Flush broadcast from {} to {}", srcTaskId, pending.dstTasks);
        }
      }

      if (list.isEmpty()) {
        broadcastPendingMap.remove(srcTaskId);
      }
    }
  }

  @Override
  public void broadcast(final String srcTaskId,
                        final String edgeId,
                        List<String> dstTasks, Serializer serializer, Object event) {
    // LOG.info("Broadcast watermark in pipeline Manager worker {}", event);

    // check channel
    final List<String> noChannelTasks =
      dstTasks.stream().filter(dstTaskId -> !getChannelForDstTask(dstTaskId, false).isPresent())
        .collect(Collectors.toList());

    if (noChannelTasks.size() > 0) {
      broadcastPendingMap.putIfAbsent(srcTaskId, new LinkedList<>());
      broadcastPendingMap.get(srcTaskId).add(new BroadcastPending(edgeId, dstTasks, serializer, event));
      return;
    }

    flushNoChannelBroadcast(srcTaskId);
    broadcastInternal(srcTaskId, edgeId, dstTasks, serializer, event);
  }

  @Override
  public void broadcast(String srcTaskId, String edgeId, List<String> dstTasks, ByteBuf byteBuf) {
    // LOG.info("Broadcast watermark in pipeline Manager worker {}", event);
    final Map<String, List<Integer>> executorDstTaskIndicesMap = new HashMap<>();

    dstTasks.forEach(dstTask -> {
      final String executorId = taskScheduledMapWorker.getRemoteExecutorId(dstTask, false);
      executorDstTaskIndicesMap.putIfAbsent(executorId, new LinkedList<>());
      executorDstTaskIndicesMap.get(executorId).add(
        pipeIndexMapWorker.getPipeIndex(srcTaskId, edgeId, dstTask));
    });

    for (final String remoteExecutorId : executorDstTaskIndicesMap.keySet()) {
      final ContextManager contextManager = byteTransfer.getRemoteExecutorContetxManager(remoteExecutorId);
      final Channel channel = contextManager.getChannel();

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
          synchronized (pendingOutputPipeMap) {
            if (pendingOutputPipeMap.containsKey(pendingIndex)) {
              pendingOutputPipeMap.get(pendingIndex).add(
                DataFrameEncoder.DataFrame.newInstance(
                  pendingIndex, byteBuf.retainedDuplicate(), byteBuf.readableBytes(), true));
            } else {
              pipeIndices.add(pendingIndex);
            }
          }
        });
      }

      if (pipeIndices.size() > 0) {
        channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(
          pipeIndices, byteBuf.retainedDuplicate(), byteBuf.readableBytes(), true))
          .addListener(listener);
      }
    }

    byteBuf.release();
  }

  private Optional<Channel> getChannelForDstTask(final String dstTaskId,
                                                 final boolean syncToMaster) {
    if (taskScheduledMapWorker.getRemoteExecutorId(dstTaskId, syncToMaster) == null) {
      return Optional.empty();
    } else {
      return Optional.of(executorChannelManagerMap
        .getExecutorChannel(
          taskScheduledMapWorker.getRemoteExecutorId(dstTaskId, syncToMaster)));
    }
  }

  private void writeData(final int pipeIndex,
                         final ByteBuf byteBuf) {
    final Triple<String, String, String> key = pipeIndexMapWorker.getKey(pipeIndex);
    final Object finalData = DataFrameEncoder.DataFrame.newInstance(
      pipeIndex, byteBuf, byteBuf.readableBytes(), true);
    final String dstTaskId = key.getRight();

    final Optional<Channel> optional = getChannelForDstTask(dstTaskId, false);
    if (pendingOutputPipeMap.containsKey(pipeIndex) || !optional.isPresent()) {
      // this is pending output pipe
      synchronized (pendingOutputPipeMap) {
        if (pendingOutputPipeMap.containsKey(pipeIndex)) {
          pendingOutputPipeMap.get(pipeIndex).add(finalData);
          pipeOuptutIndicesForDstTask.putIfAbsent(dstTaskId, new HashSet<>());
          pipeOuptutIndicesForDstTask.get(dstTaskId).add(pipeIndex);
        } else {
          getChannelForDstTask(dstTaskId, false)
            .get().writeAndFlush(finalData).addListener(listener);
        }
      }
    } else {
      optional.get().writeAndFlush(finalData).addListener(listener);
    }
  }

  @Override
  public void writeData(final String srcTaskId,
                        final String edgeId,
                        final String dstTaskId,
                        final Serializer serializer,
                        final Object event) {
    final int index = pipeIndexMapWorker.getPipeIndex(srcTaskId, edgeId, dstTaskId);
    final Optional<Channel> optional = getChannelForDstTask(dstTaskId, false);

    ByteBuf byteBuf;
    if (pendingOutputPipeMap.containsKey(index) || !optional.isPresent()) {
      synchronized (pendingOutputPipeMap) {
        if (pendingOutputPipeMap.containsKey(index)) {
          byteBuf = ByteBufAllocator.DEFAULT.ioBuffer();
        } else {
          final Channel channel = optional.get();
          byteBuf = channel.alloc().ioBuffer();
        }
      }
    } else {
      final Channel channel = optional.get();
      byteBuf = channel.alloc().ioBuffer();
    }

    final ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(byteBuf);
    try {
      final OutputStream wrapped = byteBufOutputStream;
      final EncoderFactory.Encoder encoder = serializer.getEncoderFactory().create(wrapped);
      encoder.encode(event);
      wrapped.close();
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    writeData(index, byteBuf);
  }

  @Override
  public void writeData(String srcTaskId, String edgeId, String dstTaskId, ByteBuf event) {
    final int index = pipeIndexMapWorker.getPipeIndex(srcTaskId, edgeId, dstTaskId);
    writeData(index, event);
  }

  @Override
  public void addInputData(final int index, ByteBuf event) {
    if (!inputPipeIndexInputReaderMap.containsKey(index)) {
      throw new RuntimeException("Invalid task index " + index);
    }
    inputPipeIndexInputReaderMap.get(index).addData(index, event);
  }

  @Override
  public void addControlData(int index, TaskControlMessage controlMessage) {
     if (!inputPipeIndexInputReaderMap.containsKey(index)) {
       throw new RuntimeException("Invalid task index " + index);
     }
     inputPipeIndexInputReaderMap.get(index).addControl(controlMessage);
  }

  @Override
  public synchronized void stopOutputPipe(int index, String taskId) {
    if (taskStoppedOutputPipeIndicesMap.containsKey(taskId)
      && taskStoppedOutputPipeIndicesMap.get(taskId).contains((Integer) index)) {
      throw new RuntimeException("Output pipe already stopped " + index + " " + taskId);
    }

    // send control message
    final Triple<String, String, String> key = pipeIndexMapWorker.getKey(index);

    // Why do not sync with master?
    // Because this should be sent to the downstream task even though it is removed from the master's scheduled map
    final Optional<Channel> optional = getChannelForDstTask(key.getRight(), false);
    if (!optional.isPresent()) {
      throw new RuntimeException("Contextmanager should exist for " + key);
    }

    final Channel channel = optional.get();

    if (taskInputPipeState.get(taskId).equals(InputPipeState.STOPPED)) {
      LOG.info("Task is already removed {} in executor {}", taskId, executorId);
      // flush data
      synchronized (pendingOutputPipeMap) {
        if (pendingOutputPipeMap.containsKey(index)) {
          pendingOutputPipeMap.get(index);
          final List<Object> pendingData = pendingOutputPipeMap.remove(index);
          pendingData.forEach(data -> channel.write(data));
          channel.flush();
        }
      }
    } else {
      pendingOutputPipeMap.putIfAbsent(index, new LinkedList<>());
      taskStoppedOutputPipeIndicesMap.putIfAbsent(taskId, new HashSet<>());
      pipeOuptutIndicesForDstTask.putIfAbsent(key.getRight(), new HashSet<>());
      pipeOuptutIndicesForDstTask.get(key.getRight()).add(index);

      synchronized (taskStoppedOutputPipeIndicesMap.get(taskId)) {
        taskStoppedOutputPipeIndicesMap.get(taskId).add(index);
      }
    }

    channel.writeAndFlush(new TaskControlMessage(
      TaskControlMessage.TaskControlMessageType.PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK,
      index,
      index,
      key.getRight(),
      null));

  }

  @Override
  public synchronized void taskScheduled(final String taskId) {
    // LOG.info("Task scheduled !! {} in executor {}, pipeOutputIndicesForDstTask: {}", taskId,  executorId, pipeOuptutIndicesForDstTask);
    if (pipeOuptutIndicesForDstTask.containsKey(taskId)) {
      final Set<Integer> indices = pipeOuptutIndicesForDstTask.remove(taskId);
      indices.forEach(index -> {
        final Triple<String, String, String> key = pipeIndexMapWorker.getKey(index);
        startOutputPipe(index, key.getLeft());
      });
    }
  }

  @Override
  public synchronized void startOutputPipe(int index, String taskId) {
    // restart pending output
    try {
      synchronized (pendingOutputPipeMap) {
        if (pendingOutputPipeMap.containsKey(index)) {
          final Triple<String, String, String> key = pipeIndexMapWorker.getKey(index);
          Optional<Channel> optional = getChannelForDstTask(key.getRight(), true);
          final String remoteExecutorId = taskScheduledMapWorker.getRemoteExecutorId(key.getRight(), false);

          if (evalConf.controlLogging) {
            LOG.info("Emit pending data from {} when pipe is initiated {} in executor {} to executor {}", taskId, key, executorId, remoteExecutorId);
          }

          if (!optional.isPresent()) {
            LOG.warn("{} is not schedule yet... we buffer the event and it will be emitted when task is scheduled in executor {}", key, executorId);
            return;
          }

          final Channel channel = optional.get();

          final List<Object> pendingData = pendingOutputPipeMap.remove(index);
          pendingData.forEach(data -> channel.write(data));

          channel.flush();

        } else {
          if (evalConf.controlLogging) {
            LOG.info("Start pipe {} from {} in executor {}", index, taskId, executorId);
          }
        }

        if (taskStoppedOutputPipeIndicesMap.containsKey(taskId)) {
          taskStoppedOutputPipeIndicesMap.get(taskId).remove((Integer) index);
          if (taskStoppedOutputPipeIndicesMap.get(taskId).isEmpty()) {
            taskStoppedOutputPipeIndicesMap.remove(taskId);
          }
        }
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized boolean isOutputPipeStopped(String taskId) {
    if (evalConf.controlLogging) {
      LOG.info("Output pipe stopped indices of {}: {} in executor {}", taskId, taskStoppedOutputPipeIndicesMap, executorId);
    }
    return taskStoppedOutputPipeIndicesMap.containsKey(taskId);
  }

  @Override
  public void flush() {
    executorChannelManagerMap.getExecutorChannels().forEach(channel -> {
      channel.flush();
    });
  }

  @Override
  public <T> CompletableFuture<T> request(int taskIndex, Object event) {
    throw new RuntimeException("Not implemented yet");
  }

  @Override
  public void close() {
    executorChannelManagerMap.getExecutorChannels().forEach(channel -> {
      channel.close();
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
