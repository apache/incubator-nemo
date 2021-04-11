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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.offloading.common.Pair;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.executor.common.datatransfer.DataFrameEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.InputReader;
import org.apache.nemo.runtime.executor.common.datatransfer.OffloadingDataFrameEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Two threads use this class
 * - Network thread: Saves pipe connections created from destination tasks.
 * - Task executor thread: Creates new pipe connections to destination tasks (read),
 *                         or retrieves a saved pipe connection (write)
 */
@ThreadSafe
public final class OffloadingPipeManagerWorkerImpl implements PipeManagerWorker {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingPipeManagerWorkerImpl.class.getName());

  private final String executorId;
  private Channel channel;
  private final Map<Triple<String, String, String>, Integer> map;

  private final Map<Integer, InputReader> inputPipeIndexInputReaderMap = new ConcurrentHashMap<>();
  public final Map<Integer, String> indexTaskMap;


  @Inject
  public OffloadingPipeManagerWorkerImpl(
    final String executorId,
    final Map<Triple<String, String, String>, Integer> map,
    final Map<Integer, String> indexTaskMap) {
    this.executorId = executorId;
    this.map = map;
    this.indexTaskMap = indexTaskMap;

  }

  public void setChannel(final Channel ch) {
    channel = ch;
  }

  @Override
  public void broadcast(String srcTaskId, String edgeId, List<String> dstTasks, Serializer serializer, Object event) {
    final ByteBuf byteBuf = channel.alloc().ioBuffer();
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

    final List<Integer> indices = new ArrayList<>(dstTasks.size());
    for (final String dst : dstTasks) {
      indices.add(map.get(Triple.of(srcTaskId, edgeId, dst)));
    }

    channel.write(OffloadingDataFrameEncoder.DataFrame.newInstance(
      indices,
      byteBuf,
      byteBuf.readableBytes()));
  }

  @Override
  public void writeData(String srcTaskId,
                        String edgeId,
                        String dstTaskId,
                        Serializer serializer,
                        Object event) {
    // redircet to original executor
    final ByteBuf byteBuf = channel.alloc().ioBuffer();
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

    final int index = map.get(Triple.of(srcTaskId, edgeId, dstTaskId));
    channel.write(OffloadingDataFrameEncoder.DataFrame.newInstance(
      Collections.singletonList(index),
      byteBuf,
      byteBuf.readableBytes()));
  }

  @Override
  public void writeByteBufData(String srcTaskId, String edgeId, String dstTaskId, ByteBuf event) {
    throw new RuntimeException("not supported");
  }

  @Override
  public void taskScheduled(String taskId) {
    throw new RuntimeException("not supported");
  }

  private final Map<String, Queue<Pair<Integer, ByteBuf>>> pendingByteBufQueueMap = new ConcurrentHashMap<>();
  private final Map<String, Queue<Pair<Integer, TaskControlMessage>>> pendingControlQueueMap = new ConcurrentHashMap<>();

  // private final AtomicInteger pendingDataNum = new AtomicInteger(0);
  private final AtomicInteger addDataNum = new AtomicInteger(0);


  public final AtomicLong byteReceived = new AtomicLong(0);

  @Override
  public void addInputData(int index, ByteBuf event) {

    byteReceived.addAndGet(event.readableBytes() + 2 + Integer.BYTES * 2);


    if (inputPipeIndexInputReaderMap.containsKey(index)) {

      if (!pendingByteBufQueueMap.isEmpty()) {
        final String taskId = indexTaskMap.get(index);

        if (pendingByteBufQueueMap.containsKey(taskId)) {
          final Queue<Pair<Integer, ByteBuf>> queue = pendingByteBufQueueMap.remove(taskId);
          if (queue != null) {
            queue.forEach(data -> {
              inputPipeIndexInputReaderMap.get(data.left()).addData(data.left(), data.right());
              // LOG.info("Add data for index {} cnt {}", data.left(), addDataNum.getAndIncrement());
            });
          }
        }

        if (pendingControlQueueMap.containsKey(taskId)) {
          final Queue<Pair<Integer, TaskControlMessage>> queue = pendingControlQueueMap.remove(taskId);
          if (queue != null) {
            queue.forEach(data -> {
              inputPipeIndexInputReaderMap.get(data.left()).addControl(data.right());
            });
          }
        }
      }

      // LOG.info("Add data for index {} task {} cnt {}", index,
      //  indexTaskMap.get(index), addDataNum.getAndIncrement());

      // addDataNum.getAndIncrement();
      inputPipeIndexInputReaderMap.get(index).addData(index, event);

    } else {
      try {

        while (!indexTaskMap.containsKey(index)) {
          try {
            Thread.sleep(5);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        final String taskId = indexTaskMap.get(index);
        pendingByteBufQueueMap.putIfAbsent(taskId, new ConcurrentLinkedQueue<>());
        final Queue<Pair<Integer, ByteBuf>> queue = pendingByteBufQueueMap.get(taskId);
        queue.add(Pair.of(index, event));
        // LOG.info("Pending data for index {} cnt {}", index, pendingDataNum.getAndIncrement());
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Task  index " + index + ", " + indexTaskMap);
      }
    }
  }

  @Override
  public void addControlData(int index, TaskControlMessage controlMessage) {

    if (inputPipeIndexInputReaderMap.containsKey(index)) {
      final String taskId = indexTaskMap.get(index);

      if (pendingControlQueueMap.containsKey(taskId)) {
        final Queue<Pair<Integer, TaskControlMessage>> queue = pendingControlQueueMap.remove(taskId);
        if (queue != null) {
          queue.forEach(data -> {
            inputPipeIndexInputReaderMap.get(data.left()).addControl(data.right());
          });
        }
      }

      inputPipeIndexInputReaderMap.get(index).addControl(controlMessage);

    } else {

      while (!indexTaskMap.containsKey(index)) {
        try {
          Thread.sleep(5);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      final String taskId = indexTaskMap.get(index);

      pendingControlQueueMap.putIfAbsent(taskId, new ConcurrentLinkedQueue<>());
      final Queue<Pair<Integer, TaskControlMessage>> queue = pendingControlQueueMap.get(taskId);
      queue.add(Pair.of(index, controlMessage));
    }
  }

  @Override
  public void setTaskStop(String taskId) {

  }

  @Override
  public void stopOutputPipe(int index, String taskId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void writeControlMessage(String srcTaskId, String edgeId, String dstTaskId, TaskControlMessage.TaskControlMessageType type) {

  }

  @Override
  public void startOutputPipe(int index, String taskId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public boolean isOutputPipeStopped(String taskId) {
    return false;
  }

  @Override
  public void flush() {

  }

  @Override
  public <T> CompletableFuture<T> request(int taskIndex, Object event) {
    return null;
  }

  @Override
  public void close() {

  }

  @Override
  public void retrieveIndexForOffloadingSource(String srcTaskId, String edgeId) {
    throw new RuntimeException("not suport");
  }

  @Override
  public void registerInputPipe(String srcTaskId,
                                String edgeId,
                                String dstTaskId,
                                InputReader inputReader) {
    // register
    final Triple<String, String, String> key = Triple.of(srcTaskId, edgeId, dstTaskId);
    final int index = map.get(key);
    inputPipeIndexInputReaderMap.put(index, inputReader);

  }

  @Override
  public void sendStopSignalForInputPipes(List<String> srcTasks, String edgeId, String dstTaskId) {

  }

  @Override
  public void receiveAckInputStopSignal(String taskId, int pipeIndex) {

  }

  @Override
  public InputPipeState getInputPipeState(String taskId) {
    return null;
  }

  @Override
  public boolean isInputPipeStopped(String taskId) {
    return false;
  }
}
