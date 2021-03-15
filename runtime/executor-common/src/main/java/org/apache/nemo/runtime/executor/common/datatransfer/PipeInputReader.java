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
package org.apache.nemo.runtime.executor.common.datatransfer;

import io.netty.buffer.ByteBuf;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Represents the input data transfer to a task.
 */
public final class PipeInputReader implements InputReader {

  private static final Logger LOG = LoggerFactory.getLogger(PipeInputReader.class.getName());



  /**
   * Attributes that specify how we should read the input.
   */
  private final IRVertex srcVertex;
  private final String taskId;
  private final RuntimeEdge runtimeEdge;
  private final Serializer serializer;
  private final ExecutorThreadQueue executorThreadQueue;

  public PipeInputReader(final IRVertex srcIRVertex,
                         final String taskId,
                         final RuntimeEdge runtimeEdge,
                         final Serializer serializer,
                         final ExecutorThreadQueue executorThreadQueue) {
    this.srcVertex = srcIRVertex;
    this.taskId = taskId;
    this.runtimeEdge = runtimeEdge;
    this.serializer = serializer;
    this.executorThreadQueue = executorThreadQueue;
  }


  @Override
  public Future<Integer> stop(final String taskId) {
    return null;
  }

  @Override
  public synchronized void restart() {
  }

  @Override
  public List<CompletableFuture<IteratorWithNumBytes>> read() {
    return null;
  }

  @Override
  public String getTaskId() {
    return taskId;
  }

  @Override
  public void addControl(TaskControlMessage message) {
    if (message.canShortcut()) {
      executorThreadQueue.addShortcutEvent(message);
    } else {
      executorThreadQueue.addEvent(message);
    }
  }

  @Override
  public void addData(final int pipeIndex, ByteBuf data) {
    executorThreadQueue.addEvent(
      new TaskHandlingDataEvent(taskId, runtimeEdge.getId(), pipeIndex, data,
        serializer.getDecoderFactory()));
  }

  @Override
  public IRVertex getSrcIrVertex() {
    return srcVertex;
  }

}
