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
package org.apache.nemo.runtime.executor.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
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
import java.util.function.Function;
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


  private final EvalConf evalConf;

  private final boolean onLambda;

  private final SerializerManager serializerManager;

  private final StreamVertexSerializerManager streamVertexSerializerManager;

  private final ScheduledExecutorService scheduledExecutorService;

  //          edge1                                               edge2
  //  T1  --(index)-->  [InputReader (T4, edge1)]  --> <T4> --> [OutputWriter]  --(index)-->   T5
  //  T2  --(index)-->                                                        --(index)-->   T6
  //  T3  --(index)-->                                                        --(index)-->   T7
  @Inject
  private PipeManagerWorkerImpl(@Parameter(JobConf.ExecutorId.class) final String executorId,
                                final ByteTransfer byteTransfer,
                                final EvalConf evalConf,
                                @Parameter(EvalConf.ExecutorOnLambda.class) final boolean onLamba,
                                final ExecutorChannelManagerMap executorChannelManagerMap,
                                final TaskScheduledMapWorker taskScheduledMapWorker,
                                final PipeIndexMapWorker pipeIndexMapWorker,
                                final StreamVertexSerializerManager streamVertexSerializerManager,
                                final SerializerManager serializerManager,
                                final TaskExecutorMapWrapper taskExecutorMapWrapper) {
    this.executorId = executorId;
    this.evalConf = evalConf;
    this.byteTransfer = byteTransfer;
    this.onLambda = onLamba;
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    this.executorChannelManagerMap = executorChannelManagerMap;
    this.taskScheduledMapWorker = taskScheduledMapWorker;
    this.pipeIndexMapWorker = pipeIndexMapWorker;
    this.serializerManager = serializerManager;
    this.streamVertexSerializerManager = streamVertexSerializerManager;
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    /*
    scheduledExecutorService.scheduleAtFixedRate(() -> {
      pendingOutputPipeMap.entrySet().forEach(entry -> {
        final int index = entry.getKey();
        final Triple<String, String, String> key = pipeIndexMapWorker.getKey(index);
        if (entry.getValue().size() < 5) {
          LOG.info("{} Pending key {}, size {}, val: {}", executorId,
            key, entry.getValue().size(), entry.getValue());
        } else {
          LOG.info("{} Pending key {}, size {}", executorId, key, entry.getValue().size());
        }
      });

    }, 1, 1, TimeUnit.SECONDS);
    */
  }


  @Override
  public void sendPipeInitMessage(final String srcTaskId,
                                final String edgeId,
                                final String dstTaskId) {
    final int inputPipeIndex = pipeIndexMapWorker.getPipeIndex(srcTaskId, edgeId, dstTaskId);
    final int outputPipeIndex = pipeIndexMapWorker.getPipeIndex(dstTaskId, edgeId, srcTaskId);

    // If the dst task is not scheduled yet
    // We should buffer the message and flush when the dst task is scheduled and pipe is initiated

    if (evalConf.controlLogging) {
      LOG.info("Send pipe init message {}->{}->{}", srcTaskId, edgeId, dstTaskId);
    }

    // A -> B [*] remoteInputPipeIndex of B == outputPipeIndex of A
    // B -> A  (outputPipeIndex) of B  == (inputPIpeInex) of A
    final TaskControlMessage controlMessage = new TaskControlMessage(
      TaskControlMessage.TaskControlMessageType.PIPE_INIT,
      outputPipeIndex, // A's input pipe index
      inputPipeIndex, // A's output pipe index
      srcTaskId,
      null);

    if (pendingOutputPipeMap.containsKey(outputPipeIndex)) {
      // downstream task and output pipe is not registered yet
      pendingOutputPipeMap.get(outputPipeIndex).add(controlMessage);
    } else {
      if (taskExecutorMapWrapper.containsTask(srcTaskId)) {
        // local task
        taskExecutorMapWrapper.getTaskExecutorThread(srcTaskId).addShortcutEvent(controlMessage);
      } else{
        // remote task
        final Optional<Channel> optional = getChannelForDstTask(srcTaskId, false);
        if (!optional.isPresent()) {
          throw new RuntimeException("Task is not scheduled for output pipe ... " +
            dstTaskId + "->" + srcTaskId
            + ", " + outputPipeIndex + " in executor " + executorId);
        }

        optional.get().writeAndFlush(controlMessage);
      }
    }
  }

  @Override
  public synchronized void registerInputPipe(final String srcTaskId,
                                final String edgeId,
                                final String dstTaskId,
                                final InputReader reader) {
    // taskId로부터 받는 data를 위한 input reader
    // LOG.info("Getting pipe index for {}", dstTaskId);
    final int inputPipeIndex = pipeIndexMapWorker.getPipeIndex(srcTaskId, edgeId, dstTaskId);
    final int outputPipeIndex = pipeIndexMapWorker.getPipeIndex(dstTaskId, edgeId, srcTaskId);

    if (pendingOutputPipeMap.containsKey(outputPipeIndex)) {
      // legacy
      if (!pendingOutputPipeMap.get(outputPipeIndex).isEmpty()) {
        throw new RuntimeException("There is pending data for " + pipeIndexMapWorker.getKey(outputPipeIndex));
      }
      pendingOutputPipeMap.remove(outputPipeIndex);
    }

    // Registering output pipe index
    if (!taskExecutorMapWrapper.containsTask(srcTaskId) &&
      // If downstream task is not scheduled yet, we should set output pending,
      // then output task should send PIPE_INIT message
      !getChannelForDstTask(srcTaskId, true).isPresent()) {
      if (evalConf.controlLogging) {
        LOG.info("Stop output pipe index {} / {}->{}", outputPipeIndex, dstTaskId, srcTaskId);
      }
      stopOutputPipe(outputPipeIndex, dstTaskId, srcTaskId);
    }

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
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private final Map<String, List<Integer>> inputStopSignalPipes = new ConcurrentHashMap<>();

  private TaskControlMessage buildControlMessage(
    final TaskControlMessage.TaskControlMessageType taskControlMessageType,
    final int myOutputPipeIndex,
    final int myInputPipeIndex,
    final String srcTask,
    final String dstTaskId,
    final String edgeId,
    final Object event) {
    switch (taskControlMessageType) {
      default: {
        return new TaskControlMessage(
          taskControlMessageType,
          myInputPipeIndex,
          myOutputPipeIndex,
          dstTaskId,
          event);
      }
    }
  }

  private void stopOutputPipe(final int outputPipeIndex,
                              final String taskIdOfOutputPipeIndex,
                              final String dstTask) {
    pendingOutputPipeMap.putIfAbsent(outputPipeIndex, new LinkedList<>());
  }

  @Override
  public void sendSignalForInputPipes(final List<String> srcTasks,
                                      final String edgeId,
                                      final String dstTaskId,
                                      final Function<Triple<Integer, Integer, String>, TaskControlMessage> messageBuilder) {
    LOG.info("Send signal for input pipes {}/{}/{}", srcTasks, edgeId, dstTaskId);
    for (final String srcTask : srcTasks) {
      final int outputPipeIndex = pipeIndexMapWorker.getPipeIndex(dstTaskId, edgeId, srcTask);
      final int myInputPipeIndex = pipeIndexMapWorker.getPipeIndex(srcTask, edgeId, dstTaskId);

      final TaskControlMessage controlMessage = messageBuilder
        .apply(Triple.of(outputPipeIndex, myInputPipeIndex, srcTask));

      if (pendingOutputPipeMap.containsKey(outputPipeIndex)) {
        LOG.warn("Pending output pipe  " + srcTask
          + "-" + edgeId + "-" + dstTaskId + " , index " + outputPipeIndex);
        pendingOutputPipeMap.get(outputPipeIndex).add(controlMessage);
        return;
      }

      taskInputPipeState.put(dstTaskId, InputPipeState.RUNNING);

      if (taskExecutorMapWrapper.containsTask(srcTask)) {
        // local task... !!!
        taskExecutorMapWrapper.getTaskExecutorThread(srcTask).addShortcutEvent(controlMessage);
      } else {
        // remote task
        final Optional<Channel> optional = getChannelForDstTask(srcTask, true);
        if (!optional.isPresent()) {
          throw new RuntimeException("Remote task does not scheduled for sending stop signal " +
            dstTaskId + " ->" + srcTask + ", " + outputPipeIndex + ", in executor " + executorId);
          // LOG.warn("Task {} is already removed... dont have to wait for the ack in executor {}", srcTask, executorId);
          // final int pipeIndex = pipeIndexMapWorker.getPipeIndex(srcTask, edgeId, dstTaskId);
          // inputStopSignalPipes.get(dstTaskId).remove((Integer) pipeIndex);
        } else {
          final Channel channel = optional.get();
          channel.writeAndFlush(controlMessage)
            .addListener(listener);
        }
      }
    }
  }

  @Override
  public synchronized void sendStopSignalForInputPipes(final List<String> srcTasks,
                                                       final String edgeId,
                                                       final String dstTaskId,
                                                       final Function<Triple<Integer, Integer, String>, TaskControlMessage> messageBuilder) {
    inputStopSignalPipes.putIfAbsent(dstTaskId, new ArrayList<>(srcTasks.size()));

    for (final String srcTask : srcTasks) {
      final int pipeIndex = pipeIndexMapWorker.getPipeIndex(srcTask, edgeId, dstTaskId);
      inputStopSignalPipes.get(dstTaskId).add(pipeIndex);
    }

    for (final String srcTask : srcTasks) {
      final int outputPipeIndex = pipeIndexMapWorker.getPipeIndex(dstTaskId, edgeId, srcTask);
      final int myInputPipeIndex = pipeIndexMapWorker.getPipeIndex(srcTask, edgeId, dstTaskId);

      final TaskControlMessage controlMessage = messageBuilder
        .apply(Triple.of(outputPipeIndex, myInputPipeIndex, srcTask));

      if (pendingOutputPipeMap.containsKey(outputPipeIndex)) {
        // src task is stopped...
        if (evalConf.controlLogging) {
          LOG.info("Output pipe {} is pending in {}.. {}->{}->{}, add control message to output pipe map",
            outputPipeIndex, executorId, dstTaskId, edgeId, srcTask);
        }
        pendingOutputPipeMap.get(outputPipeIndex).add(controlMessage);
      } else {
        taskInputPipeState.put(dstTaskId, InputPipeState.WAITING_ACK);

        if (taskExecutorMapWrapper.containsTask(srcTask)) {
          // local task... !!!
          if (evalConf.controlLogging) {
            LOG.info("Send local input stop signal for input pipes {}  src task {} index {} in executor {}",
              dstTaskId, srcTask, myInputPipeIndex, executorId);
          }

          taskExecutorMapWrapper.getTaskExecutorThread(srcTask).addShortcutEvent(controlMessage);
        } else {
          // remote task
          final Optional<Channel> optional = getChannelForDstTask(srcTask, true);
          if (!optional.isPresent()) {
            throw new RuntimeException("Remote task does not scheduled for sending stop signal " +
              dstTaskId + " ->" + srcTask + ", " + outputPipeIndex + ", in executor " + executorId);
            // LOG.warn("Task {} is already removed... dont have to wait for the ack in executor {}", srcTask, executorId);
            // final int pipeIndex = pipeIndexMapWorker.getPipeIndex(srcTask, edgeId, dstTaskId);
            // inputStopSignalPipes.get(dstTaskId).remove((Integer) pipeIndex);
          } else {
            final Channel channel = optional.get();

            if (evalConf.controlLogging) {
              LOG.info("Send input stop signal {} for input pipes src task {} my output index {} my input index {} in executor {}",
                dstTaskId, srcTask,  outputPipeIndex, myInputPipeIndex, executorId);
            }

            channel.writeAndFlush(controlMessage)
              .addListener(listener);
          }
        }
      }
    }
  }

  @Override
  public void setTaskStop(String taskId) {
    taskInputPipeState.put(taskId, InputPipeState.STOPPED);
  }

  @Override
  public void receiveAckInputStopSignal(String taskId, int inputPipeIndex) {

    if (!inputStopSignalPipes.containsKey(taskId)) {
      throw new RuntimeException("Invalid pipe stop ack " + taskId + ", " + inputPipeIndex);
    }

    if (evalConf.controlLogging) {
      LOG.info("Receive act input stop signal for {} index {} in executor {}", taskId, inputPipeIndex, executorId);
    }

    final List<Integer> stopPipes = inputStopSignalPipes.get(taskId);
    stopPipes.remove((Integer) inputPipeIndex);

    // add pending output pipe index
    final Triple<String, String, String> t = pipeIndexMapWorker.getKey(inputPipeIndex);
    final int outputPipeIndex = pipeIndexMapWorker.getPipeIndex(t.getRight(), t.getMiddle(), t.getLeft());
    stopOutputPipe(outputPipeIndex, t.getRight(), t.getLeft());

    if (stopPipes.isEmpty()) {
      inputStopSignalPipes.remove(taskId);
      taskInputPipeState.put(taskId, InputPipeState.STOPPED);
    }
  }

  @Override
  public synchronized InputPipeState getInputPipeState(String taskId) {
    return taskInputPipeState.get(taskId);
  }

  @Override
  public boolean isInputPipeStopped(String taskId) {
    if (evalConf.controlLogging) {
      // LOG.info("TaskInputPipeState in executor {}, requesting {}", executorId, taskId);
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
      String remoteExecutorId = taskScheduledMapWorker.getRemoteExecutorId(dstTask, false);
      if (remoteExecutorId == null || remoteExecutorId.equals("null")) {
        if (taskExecutorMapWrapper.containsTask(dstTask)) {
          remoteExecutorId = executorId;
        } else {
          throw new RuntimeException("executor id is null for task " + dstTask);
        }
      }
      executorDstTaskIndicesMap.putIfAbsent(remoteExecutorId, new LinkedList<>());
      executorDstTaskIndicesMap.get(remoteExecutorId).add(
        pipeIndexMapWorker.getPipeIndex(srcTaskId, edgeId, dstTask));
    });

    for (final String remoteExecutorId : executorDstTaskIndicesMap.keySet()) {

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
          if (pendingOutputPipeMap.containsKey(pendingIndex)) {
            pendingOutputPipeMap.get(pendingIndex).add(new PendingPair(event, serializer));
            // DataFrameEncoder.DataFrame.newInstance(
            //  pendingIndex, byteBuf.retainedDuplicate(), byteBuf.readableBytes(), true));
          } else {
            pipeIndices.add(pendingIndex);
          }
        });
      }

      if (pipeIndices.size() > 0) {
        if (remoteExecutorId.equals(executorId)) {
          // local
          for (final int index : pipeIndices) {
            final Triple<String, String, String> key = pipeIndexMapWorker.getKey(index);
            if (taskExecutorMapWrapper.containsTask(key.getRight())) {
              sendLocalToLocal(key.getRight(), key.getMiddle(), index, event);
            } else {
              // remote
              final String remote = taskScheduledMapWorker.getRemoteExecutorId(key.getRight(), true);
              final Channel channel = executorChannelManagerMap
                .getExecutorChannel(remote);
              sendLocalToRemote(channel, index, serializer, event);
            }
          }
        } else {
          // remote
          final Channel channel = executorChannelManagerMap
            .getExecutorChannel(remoteExecutorId);
          sendLocalToRemote(channel, pipeIndices, serializer, event);
        }

      }
    }
  }

  private void sendLocalToRemote(final Channel channel,
                                 final int pipeIndex,
                                 final Serializer serializer,
                                 final Object event) {
    final ByteBuf byteBuf = channel.alloc().ioBuffer();
    final ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(byteBuf);

    // a -> local stream vertex -> remote vertex
    // we should encode the data with the a-> edge serializer
    try {
      final OutputStream wrapped = byteBufOutputStream;
      //DataUtil.buildOutputStream(byteBufOutputStream, serializer.getEncodeStreamChainers());

      final EncoderFactory.Encoder encoder = serializer.getEncoderFactory().create(wrapped);
      //LOG.info("Element encoder: {}", encoder);
      encoder.encode(event);
      wrapped.close();

      channel.write(DataFrameEncoder.DataFrame.newInstance(
        pipeIndex, byteBuf, byteBuf.readableBytes(), true))
        .addListener(listener);
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private void sendLocalToRemote(final Channel channel,
                                 final List<Integer> pipeIndices,
                                 final Serializer serializer,
                                 final Object event) {
    final ByteBuf byteBuf = channel.alloc().ioBuffer();
    final ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(byteBuf);

    // a -> local stream vertex -> remote vertex
    // we should encode the data with the a-> edge serializer
    try {
      final OutputStream wrapped = byteBufOutputStream;
      //DataUtil.buildOutputStream(byteBufOutputStream, serializer.getEncodeStreamChainers());

      final EncoderFactory.Encoder encoder = serializer.getEncoderFactory().create(wrapped);
      //LOG.info("Element encoder: {}", encoder);
      encoder.encode(event);
      wrapped.close();

      if (pipeIndices.size() > 1) {
        channel.write(DataFrameEncoder.DataFrame.newInstance(
          pipeIndices, byteBuf, byteBuf.readableBytes(), true))
          .addListener(listener);
      } else {
        channel.write(DataFrameEncoder.DataFrame.newInstance(
          pipeIndices.get(0), byteBuf, byteBuf.readableBytes(), true))
          .addListener(listener);
      }
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private void sendLocalToLocal(final String dstTaskId,
                                final String edgeId,
                                final int index,
                                final Object event) {
    taskExecutorMapWrapper.getTaskExecutorThread(dstTaskId)
      .handlingDataEvent(new TaskLocalDataEvent(dstTaskId, edgeId, index, event));
  }

  private final Map<String, List<BroadcastPending>> broadcastPendingMap = new ConcurrentHashMap<>();

  private void flushNoChannelBroadcast(final String srcTaskId) {
    if (broadcastPendingMap.containsKey(srcTaskId)) {
      final List<BroadcastPending> list = broadcastPendingMap.get(srcTaskId);
      final Iterator<BroadcastPending> iterator = list.iterator();
      while (iterator.hasNext()) {
        final BroadcastPending pending = iterator.next();
        if (pending.dstTasks.stream()
          .noneMatch(dstTaskId -> !taskExecutorMapWrapper.containsTask(dstTaskId) &&
            !getChannelForDstTask(dstTaskId, false).isPresent())) {
          iterator.remove();
          broadcastInternal(srcTaskId, pending.edgeId, pending.dstTasks, pending.serializer, pending.event);
          // LOG.info("Flush broadcast from {} to {}", srcTaskId, pending.dstTasks);
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
      dstTasks.stream().filter(dstTaskId ->
        !taskExecutorMapWrapper.containsTask(dstTaskId) &&
        !getChannelForDstTask(dstTaskId, false).isPresent())
        .collect(Collectors.toList());

    if (noChannelTasks.size() > 0) {
      broadcastPendingMap.putIfAbsent(srcTaskId, new LinkedList<>());
      broadcastPendingMap.get(srcTaskId).add(new BroadcastPending(edgeId, dstTasks, serializer, event));
      return;
    }

    flushNoChannelBroadcast(srcTaskId);
    broadcastInternal(srcTaskId, edgeId, dstTasks, serializer, event);
  }

  private Optional<Channel> getChannelForDstTask(final String dstTaskId,
                                                 final boolean syncToMaster) {
    try {
      final String remoteExecutorId = taskScheduledMapWorker.getRemoteExecutorId(dstTaskId, syncToMaster);
      if (remoteExecutorId == null) {
        return Optional.empty();
      } else {
        if (remoteExecutorId.equals(executorId)) {
          return Optional.empty();
        } else {
          final Channel channel = executorChannelManagerMap.getExecutorChannel(remoteExecutorId);
          if (channel == null) {
            return Optional.empty();
          } else {
            return Optional.of(channel);
          }
        }
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Exception in getChannelForDstTask " + dstTaskId + " in " + executorId);
    }
  }

  private void sendRemoteToLocal(final String dstTaskId,
                                 final String edgeId,
                                 final int index,
                                 final ByteBuf event) {
    taskExecutorMapWrapper.getTaskExecutorThread(dstTaskId)
      .handlingDataEvent(new TaskHandlingDataEvent(dstTaskId,
        edgeId,
        index,
        event,
        serializerManager.getSerializer(edgeId).getDecoderFactory()));
  }

  private void sendRemoteToRemote(final int pipeIndex,
                                  final ByteBuf event,
                                  final Channel channel) {
    try {
      channel.write(DataFrameEncoder.DataFrame.newInstance(
        pipeIndex, event, event.readableBytes(), true))
        .addListener(listener);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private void sendControlToLocal(final String dstTaskId,
                                  final TaskControlMessage msg) {
    taskExecutorMapWrapper.getTaskExecutorThread(dstTaskId)
      .handlingControlEvent(msg);
  }

  private void sendControlToRemote(final Channel channel,
                                   final TaskControlMessage msg) {
    channel.writeAndFlush(msg);
  }

  @Override
  public void writeControlMessage(final String srcTaskId,
                                  final String edgeId,
                                  final String dstTaskId,
                                  final TaskControlMessage.TaskControlMessageType type,
                                  final Object event) {
    final int index = pipeIndexMapWorker.getPipeIndex(srcTaskId, edgeId, dstTaskId);
    final TaskControlMessage msg = buildControlMessage(type,
      index, index, srcTaskId, dstTaskId, edgeId, event);

    if (pendingOutputPipeMap.containsKey(index)) {
      LOG.info("Added control message to pending output pipe {}/{}/{}", srcTaskId, edgeId, dstTaskId, type);
      pendingOutputPipeMap.get(index).add(msg);
    } else {
      if (taskExecutorMapWrapper.containsTask(dstTaskId)) {
        // local task
        sendControlToLocal(dstTaskId, msg);
      } else {
        final Optional<Channel> optional = getChannelForDstTask(dstTaskId, false);
        if (!optional.isPresent()) {
          throw new RuntimeException("Remote task does not scheduled for sending control message " +
            srcTaskId + " ->" + dstTaskId + ", " + index + ", in executor " + executorId);
        } else {
          sendControlToRemote(optional.get(), msg);
        }
      }
    }
  }

  @Override
  public void startOutputPipeForRerouting(final String srcTaskId,
                                          final String edgeId,
                                          final String dstTaskId,
                                          final TaskControlMessage.TaskControlMessageType type) {
    final int index = pipeIndexMapWorker.getPipeIndex(srcTaskId, edgeId, dstTaskId);
    final TaskControlMessage msg = buildControlMessage(type,
      index, index, srcTaskId, dstTaskId, edgeId, null);

    if (taskExecutorMapWrapper.containsTask(dstTaskId)) {
      // local task
      sendControlToLocal(dstTaskId, msg);
    } else {
      final Optional<Channel> optional = getChannelForDstTask(dstTaskId, false);
      if (!optional.isPresent()) {
        throw new RuntimeException("Remote task does not scheduled for sending control message " +
          srcTaskId + " ->" + dstTaskId + ", " + index + ", in executor " + executorId);
      } else {
        sendControlToRemote(optional.get(), msg);
      }
    }

    startOutputPipe(index, srcTaskId);
  }

  // Write local data.
  // Data should not be the type of ByteBuf
  @Override
  public void writeData(final String srcTaskId,
                        final String edgeId,
                        final String dstTaskId,
                        final Serializer serializer,
                        final Object event) {
    final int index = pipeIndexMapWorker.getPipeIndex(srcTaskId, edgeId, dstTaskId);

    if (pendingOutputPipeMap.containsKey(index)) {
      pendingOutputPipeMap.get(index).add(new PendingPair(event, serializer));
    } else {
      if (taskExecutorMapWrapper.containsTask(dstTaskId)) {
        sendLocalToLocal(dstTaskId, edgeId, index, event);
      } else {
        final Optional<Channel> optional = getChannelForDstTask(dstTaskId, false);
        if (!optional.isPresent()) {
          throw new RuntimeException("Remote task does not scheduled for sending data message " +
            srcTaskId + " ->" + dstTaskId + ", " + index + ", in executor " + executorId);
        } else {
          sendLocalToRemote(optional.get(), index, serializer, event);
        }
      }
    }
  }

  // This is for writing byte data to the next task
  @Override
  public void writeByteBufData(String srcTaskId, String edgeId, String dstTaskId, ByteBuf byteBuf) {
    final int index = pipeIndexMapWorker.getPipeIndex(srcTaskId, edgeId, dstTaskId);

    if (pendingOutputPipeMap.containsKey(index)) {
      pendingOutputPipeMap.get(index).add(byteBuf);
    } else {
      if (taskExecutorMapWrapper.containsTask(dstTaskId)) {
        sendRemoteToLocal(dstTaskId, edgeId, index, byteBuf);
      } else {
        final Optional<Channel> optional = getChannelForDstTask(dstTaskId, false);
        if (!optional.isPresent()) {
          throw new RuntimeException("Remote task does not scheduled for sending bytebuf message " +
            srcTaskId + " ->" + dstTaskId + ", " + index + ", in executor " + executorId);
        } else {
          sendRemoteToRemote(index, byteBuf, optional.get());
        }
      }
    }
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
  public void stopOutputPipeForRouting(final int index,
                                       final String taskId) {
    if (taskStoppedOutputPipeIndicesMap.containsKey(taskId)
      && taskStoppedOutputPipeIndicesMap.get(taskId).contains((Integer) index)) {
      LOG.warn("Output pipe already stopped " + index + " " + taskId);
    }

    if (pendingOutputPipeMap.containsKey(index)) {
      LOG.warn("Output pipe already stopped " + index + " " + taskId);
    }

    // send control message
    final Triple<String, String, String> key = pipeIndexMapWorker.getKey(index);

    // stop output pipe
    stopOutputPipe(index, taskId, key.getRight());

    LOG.info("Stop output pipe {}", key);

    taskStoppedOutputPipeIndicesMap.putIfAbsent(taskId, new HashSet<>());
    taskStoppedOutputPipeIndicesMap.get(taskId).add(index);
  }

  @Override
  public synchronized void stopOutputPipe(int index, String taskId) {
    if (taskStoppedOutputPipeIndicesMap.containsKey(taskId)
      && taskStoppedOutputPipeIndicesMap.get(taskId).contains((Integer) index)) {
      LOG.warn("Output pipe already stopped " + index + " " + taskId);
    }

    if (pendingOutputPipeMap.containsKey(index)) {
      LOG.warn("Output pipe already stopped " + index + " " + taskId);
    }

    // send control message
    final Triple<String, String, String> key = pipeIndexMapWorker.getKey(index);

    // stop output pipe
    stopOutputPipe(index, taskId, key.getRight());

    taskStoppedOutputPipeIndicesMap.putIfAbsent(taskId, new HashSet<>());
    taskStoppedOutputPipeIndicesMap.get(taskId).add(index);

    final TaskControlMessage message = new TaskControlMessage(
      TaskControlMessage.TaskControlMessageType.PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK,
      index,
      index,
      key.getRight(),
      null);

    if (taskExecutorMapWrapper.containsTask(key.getRight())) {
      // local task
      taskExecutorMapWrapper.getTaskExecutorThread(key.getRight())
        .addEvent(message);
    } else {
      // Why do not sync with master?
      // Because this should be sent to the downstream task even though it is removed from the master's scheduled map
      Optional<Channel> optional = getChannelForDstTask(key.getRight(), false);
      if (!optional.isPresent()) {
        optional = getChannelForDstTask(key.getRight(), true);
        if (!optional.isPresent()) {
          throw new RuntimeException("Contextmanager should exist for " + key);
        }
      }
      final Channel channel = optional.get();

      channel.writeAndFlush(message);
    }
  }

  private void sendPendingDataToLocal(
    String dstTaskid, String edgeId,
    int index, Object data) {
    if (data instanceof TaskControlMessage) {
      taskExecutorMapWrapper.getTaskExecutorThread(dstTaskid)
        .handlingControlEvent((TaskControlMessage) data);
    } else {
      if (data instanceof ByteBuf) {
        sendRemoteToLocal(dstTaskid, edgeId, index, (ByteBuf) data);
      } else if (data instanceof PendingPair) {
        sendLocalToLocal(dstTaskid, edgeId, index, ((PendingPair)data).event);
      } else {
        sendLocalToLocal(dstTaskid, edgeId, index, data);
      }
    }
  }

  private void sendPendingDataToChannel(final int index,
                                        Object data, Channel channel) {
    if (data instanceof TaskControlMessage) {
      channel.write(data);
    } else {
      if (data instanceof ByteBuf) {
        sendRemoteToRemote(index, (ByteBuf) data, channel);
      } else if (data instanceof PendingPair) {
        final PendingPair pendingPair = (PendingPair)data;
        sendLocalToRemote(channel,
          index, pendingPair.serialier, pendingPair.event);
      } else {
        throw new RuntimeException("Not supported");
      }
    }
  }

  @Override
  public void taskScheduled(final String srcTaskId, final String dstTaskId) {
    pipeIndexMapWorker.getIndexMap().keySet().stream()
      .filter(triple -> triple.getLeft().equals(srcTaskId) && triple.getRight().equals(dstTaskId))
      .forEach(triple -> {
        final int index = pipeIndexMapWorker.getPipeIndex(triple.getLeft(), triple.getMiddle(), triple.getRight());
        LOG.info("Task {} scheduled .. start output pipe {}/{}", dstTaskId, index, triple);
        startOutputPipe(index, srcTaskId);
      });
  }

  @Override
  public void startOutputPipe(int index, String taskId) {
    // restart pending output
    try {
      final Triple<String, String, String> key = pipeIndexMapWorker.getKey(index);

      if (pendingOutputPipeMap.containsKey(index)) {
        if (taskExecutorMapWrapper.containsTask(key.getRight())) {
          // local
          if (evalConf.controlLogging) {
            LOG.info("Emit pending data from local {} when pipe is initiated {} in executor {} to local {}",
              taskId, key, executorId, key.getRight());
          }

          final List<Object> pendingData = pendingOutputPipeMap.remove(index);
          if (evalConf.controlLogging) {
            LOG.info("Send pending data to local {}/{}/{}", key.getRight(), key.getMiddle(), index);
          }
          pendingData.forEach(data ->
            sendPendingDataToLocal(key.getRight(), key.getMiddle(), index, data));
        } else {
          // remote
          if (evalConf.controlLogging) {
            LOG.info("Emit pending data from {} when pipe is initiated {} in executor {} to "
              , taskId, key, executorId);
          }

          Optional<Channel> optional = getChannelForDstTask(key.getRight(), true);
          long prevLog = System.currentTimeMillis();
          final long st = System.currentTimeMillis();
          while (!optional.isPresent()) {
            Thread.sleep(100);
            optional = getChannelForDstTask(key.getRight(), true);
            if (System.currentTimeMillis() - prevLog >= 1000) {
              LOG.warn("Waiting for get channel of task {}....", key.getRight());
              prevLog = System.currentTimeMillis();
            }

            if (System.currentTimeMillis() - st >= 15000) {
              throw new RuntimeException("Cannot connect to task " + key.getRight());
            }
          }

          final List<Object> pendingData = pendingOutputPipeMap.remove(index);

          if (pendingData.isEmpty()) {
            LOG.info("No pending data for {} in {}", key, executorId);
            return;
          }

          if (!optional.isPresent()) {
            throw new RuntimeException(key + " is not schedule yet... we buffer the event and " +
              "it will be emitted when task is scheduled in executor " + executorId);
          }

          final Channel channel = optional.get();

          if (evalConf.controlLogging) {
            LOG.info("Send pending data to channel {}/{}/{}", key.getRight());
          }
          pendingData.forEach(data -> sendPendingDataToChannel(index, data, channel));

          channel.flush();
        }

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

  final class PendingPair {
    public final Object event;
    public final Serializer serialier;
    public PendingPair(final Object event, final Serializer serializer) {
      this.event = event;
      this.serialier = serializer;
    }
  }
}
