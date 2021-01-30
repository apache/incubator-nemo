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
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;

import javax.annotation.concurrent.ThreadSafe;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Two threads use this class
 * - Network thread: Saves pipe connections created from destination tasks.
 * - Task executor thread: Creates new pipe connections to destination tasks (read),
 *                         or retrieves a saved pipe connection (write)
 */
@ThreadSafe
public interface PipeManagerWorker extends InputPipeRegister {

  void broadcast(final String srcTaskId,
                 final String edgeId,
                 final List<String> dstTasks,
                 final Serializer serializer, Object event);

  void writeData(final String srcTaskId,
                 final String edgeId,
                 final String dstTaskId,
                 final Serializer serializer,
                 final Object event);

  void taskScheduled(final String taskId);

  void addInputData(int index, ByteBuf event);

  void addControlData(int index, TaskControlMessage controlMessage);

  void stopOutputPipe(int index, String taskId);

  // When input pipe is initiated
  void startOutputPipe(int index, String taskId);

  boolean isOutputPipeStopped(String taskId);

  // flush data
  void flush();

  <T> CompletableFuture<T> request(int taskIndex, Object event);

  void close();
}
