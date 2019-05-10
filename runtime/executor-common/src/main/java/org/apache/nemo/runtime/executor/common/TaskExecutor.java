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
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.*;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Executes a task.
 * Should be accessed by a single thread.
 */
public interface TaskExecutor extends AutoCloseable {

  public enum Status {
    RUNNING,
    OFFLOAD_PENDING,
    OFFLOADED,
    DEOFFLOAD_PENDING
  }

  public void setOffloadedTaskTime(final long t);

  ConcurrentLinkedQueue<Object> getOffloadingQueue();

  AtomicLong getTaskExecutionTime();

  OutputCollector getVertexOutputCollector(String vertexId);

  long calculateOffloadedTaskTime();

  long getThreadId();

  boolean isRunning();

  boolean isOffloadPending();

  boolean isOffloaded();

  boolean isDeoffloadPending();

  String getId();

  boolean isStateless();

  AtomicInteger getProcessedCnt();

  AtomicLong getPrevOffloadStartTime();

  AtomicLong getPrevOffloadEndTime();

  void startOffloading(final long baseTime);

  void endOffloading();

  void execute();

  void sendToServerless(final Object event,
                               final List<String> nextOperatorIds,
                               final long wm,
                               final String edgeId);

  /**
   * This method is non-blocking call and only process one event.
   * Executor should call this function.
   * @return true if an event is processed
   */
  boolean handleData();

  ////////////////////////////////////////////// Misc

  void setIRVertexPutOnHold(final IRVertex irVertex);
}
