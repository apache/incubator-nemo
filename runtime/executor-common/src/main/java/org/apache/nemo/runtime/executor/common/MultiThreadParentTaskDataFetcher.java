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
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.*;

/**
 * Task thread -> fetchDataElement() -> (((QUEUE))) <- List of iterators <- queueInsertionThreads
 *
 * Unlike, where the task thread directly consumes (and blocks on) iterators one by one,
 * this class spawns threads that each forwards elements from an iterator to a global queue.
 *
 * This class should be used when dealing with unbounded data streams, as we do not want to be blocked on a
 * single unbounded iterator forever.
 */
@NotThreadSafe
public final class MultiThreadParentTaskDataFetcher extends DataFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(MultiThreadParentTaskDataFetcher.class);

  private final String taskId;

  public MultiThreadParentTaskDataFetcher(final String taskId,
                                          final IRVertex dataSource,
                                          final RuntimeEdge edge,
                                          final OutputCollector outputCollector) {
    super(dataSource, edge, outputCollector);
    this.taskId = taskId;
  }

  @Override
  public boolean hasData() {
    throw new RuntimeException("Not supported");
  }

  @Override
  public void prepare() {

  }

  @Override
  public boolean isAvailable() {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Object fetchDataElement() throws IOException, NoSuchElementException {
    throw new RuntimeException("Not supported");
  }

  @Override
  public Future<Integer> stop(final String taskId) {
    // executorGlobalInstances.deregisterWatermarkService(getDataSource());
    // return readersForParentTask.stop(taskId);
    return null;
  }

  @Override
  public void restart() {
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public String toString() {
    return "dataFetcher" + taskId + "-" + edge.getId();
  }
}
