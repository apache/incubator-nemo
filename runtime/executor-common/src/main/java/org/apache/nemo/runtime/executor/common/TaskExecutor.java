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
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.*;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.executor.common.datatransfer.IteratorWithNumBytes;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Executes a task.
 * Should be accessed by a single thread.
 */
public interface TaskExecutor extends AutoCloseable {


  public enum PendingState {
    WORKER_PENDING,
    TASK_READY_PENDING,
    INPUT_PENDING,
    OUTPUT_PENDING,
    OTHER_TASK_WAITING,
    DONE,
  }

  public enum Status {
    RUNNING,
    OFFLOAD_PENDING,
    OFFLOADED,
    DEOFFLOAD_PENDING
  }

  default void callTaskOffloadingDone() {
    // do nothing
  };

  // it is used for deleting the task in lambda to check
  // whether it is deleted for moving to vm scaling or not
  boolean deleteForMoveToVmScaling();
  void setDeleteForMoveToVmScaling(boolean v);

  public int getNumKeys();

  TaskMetrics getTaskMetrics();

  ExecutorThread getExecutorThread();

  boolean isSource();

  boolean isSourceAvailable();

  PendingState getPendingStatus();

  boolean isFinished();

  void finish();

  // after finish called
  boolean isFinishDone();

  public void setOffloadedTaskTime(final long t);

  AtomicLong getTaskExecutionTime();

  OutputCollector getVertexOutputCollector(String vertexId);

  long calculateOffloadedTaskTime();

  long getThreadId();

  boolean isRunning();

  boolean isOffloadPending();

  boolean isOffloaded();

  boolean isDeoffloadPending();

  String getId();

  //boolean hasData();

  boolean isStateless();

  AtomicInteger getProcessedCnt();

  AtomicLong getPrevOffloadStartTime();

  AtomicLong getPrevOffloadEndTime();

  void startOffloading(final long baseTime,
                       final Object worker,
                       EventHandler<Integer> offloadingDoneHandler);

  void endOffloading(final EventHandler<Integer> endOffloadingHandler,
                     boolean moveToVmScaling);

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
  //int handleData();

  boolean handleSourceData();
  void handleIntermediateWatermarkEvent(final Object event, DataFetcher dataFetcher);
  void handleIntermediateData(final IteratorWithNumBytes iterator, DataFetcher dataFetcher);
  void handleOffloadingEvent(final Object data);

  ////////////////////////////////////////////// Misc

  void setIRVertexPutOnHold(final IRVertex irVertex);
}
