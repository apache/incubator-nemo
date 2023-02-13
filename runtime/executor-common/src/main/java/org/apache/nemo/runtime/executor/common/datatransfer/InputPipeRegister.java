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
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Two threads use this class
 * - Network thread: Saves pipe connections created from destination tasks.
 * - Task executor thread: Creates new pipe connections to destination tasks (read),
 *                         or retrieves a saved pipe connection (write)
 */
@ThreadSafe
public interface InputPipeRegister {
  public enum Signal {
    INPUT_STOP,
    INPUT_START,
  }

  enum InputPipeState {
    WAITING_ACK,
    STOPPED,
    RUNNING
  }

  void sendPipeInitMessage(final String srcTaskId,
                           final String edgeId,
                           final String dstTaskId);

  void registerInputPipe(final String srcTaskId,
                         final String edgeId,
                         final String dstTaskId,
                         InputReader inputReader);

  void sendStopSignalForInputPipes(final List<String> srcTasks,
                                   final String edgeId,
                                   final String dstTaskId,
                                   final Function<Triple<Integer, Integer, String>, TaskControlMessage> messageBuilder);


  void sendSignalForInputPipes(final List<String> srcTasks,
                               final String edgeId,
                               final String dstTaskId,
                               final Function<Triple<Integer, Integer, String>, TaskControlMessage> messageBuilder);

  // return true if the all of the pipe is stopped.
  void receiveAckInputStopSignal(String taskId, int pipeIndex);

  InputPipeState getInputPipeState(String taskId);

  boolean isInputPipeStopped(String taskId);
}