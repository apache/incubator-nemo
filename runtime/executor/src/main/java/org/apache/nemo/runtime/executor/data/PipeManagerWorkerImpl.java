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
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.executor.ExecutorContextManagerMap;
import org.apache.nemo.runtime.executor.PipeIndexMapWorker;
import org.apache.nemo.runtime.executor.TaskScheduledMapWorker;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransfer;
import org.apache.nemo.runtime.executor.common.Serializer;
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
  private final Map<Integer, InputReader> taskInputReaderMap = new ConcurrentHashMap<>();

  @Inject
  private PipeManagerWorkerImpl(@Parameter(JobConf.ExecutorId.class) final String executorId,
                                final ByteTransfer byteTransfer,
                                final ExecutorContextManagerMap executorContextManagerMap,
                                final TaskScheduledMapWorker taskScheduledMapWorker,
                                final PipeIndexMapWorker pipeIndexMapWorker,
                                @Parameter(EvalConf.FlushPeriod.class) final int flushPeriod) {
    this.executorId = executorId;
    this.byteTransfer = byteTransfer;
    this.executorContextManagerMap = executorContextManagerMap;
    this.taskScheduledMapWorker = taskScheduledMapWorker;
    this.pipeIndexMapWorker = pipeIndexMapWorker;
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void registerInputPipe(final String srcTaskId,
                                final String edgeId,
                                final String dstTaskId,
                                final InputReader reader) {
    // taskId로부터 받는 data를 위한 input reader
    final int taskIndex = pipeIndexMapWorker.getPipeIndex(srcTaskId, edgeId, dstTaskId);

    LOG.info("Registering pipe for input reader {}/{}/{} . index: {}",
      srcTaskId, edgeId, dstTaskId, taskIndex);

    if (taskInputReaderMap.containsKey(taskIndex)) {
      throw new RuntimeException("Task is already registered " + taskIndex);
    }

    taskInputReaderMap.put(taskIndex, reader);
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

      channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(
        executorDstTaskIndicesMap.get(remoteExecutorId), byteBuf, byteBuf.readableBytes(), true))
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
    final ContextManager contextManager = getContextManagerForDstTask(dstTaskId);
    final Channel channel = contextManager.getChannel();

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

    final int index = pipeIndexMapWorker.getPipeIndex(srcTaskId, edgeId, dstTaskId);
    // LOG.info("Write {}->{} / {}", srcTaskId, dstTaskId, event);
    channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(
      Collections.singletonList(index), byteBuf, byteBuf.readableBytes(), true))
      .addListener(listener);
  }

  @Override
  public void addInputData(final int index, ByteBuf event) {
    if (!taskInputReaderMap.containsKey(index)) {
      throw new RuntimeException("Invalid task index " + index);
    }
    taskInputReaderMap.get(index).addData(event);
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
