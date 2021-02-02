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
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.executor.common.datatransfer.DataFrameEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.InputReader;
import org.apache.nemo.runtime.executor.common.datatransfer.OffloadingDataFrameEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


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


  public OffloadingPipeManagerWorkerImpl(
    final String executorId,
    final Map<Triple<String, String, String>, Integer> map) {
    this.executorId = executorId;
    this.map = map;
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
  public void broadcast(String srcTaskId, String edgeId, List<String> dstTasks, ByteBuf event) {
    throw new RuntimeException("not supported");
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
  public void writeData(String srcTaskId, String edgeId, String dstTaskId, ByteBuf event) {
    throw new RuntimeException("not supported");
  }

  @Override
  public void taskScheduled(String taskId) {
    throw new RuntimeException("not supported");
  }

  @Override
  public void addInputData(int index, ByteBuf event) {
    inputPipeIndexInputReaderMap.get(index).addData(index, event);
  }

  @Override
  public void addControlData(int index, TaskControlMessage controlMessage) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void setTaskStop(String taskId) {

  }

  @Override
  public void stopOutputPipe(int index, String taskId) {
    throw new RuntimeException("Not supported");
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
