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
package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.runtime.executor.data.DataUtil;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Represents the input data transfer to a task.
 */
public interface InputReader {
  /**
   * Reads input data depending on the communication pattern of the srcVertex.
   *
   * @return the list of iterators.
   */
  List<CompletableFuture<DataUtil.IteratorWithNumBytes>> read();

   /** Reads input data depending on the communication pattern of the srcVertex.
   *
     * @return the list of iterators.
    */
  List<CompletableFuture<DataUtil.IteratorWithNumBytes>> read(boolean enableWorkStealing, int maxSplitNum);

  /**
   * Retry reading input data.
   *
   * @param index of the failed iterator in the list returned by read().
   * @return the retried iterator.
   */
  CompletableFuture<DataUtil.IteratorWithNumBytes> retry(int index);

  IRVertex getSrcIrVertex();

  ExecutionPropertyMap<EdgeExecutionProperty> getProperties();

  static int getSourceParallelism(final InputReader inputReader) {
    return inputReader.getSrcIrVertex().getPropertyValue(ParallelismProperty.class)
      .orElseThrow(() -> new IllegalStateException(inputReader.getSrcIrVertex().getId()));
  }
}
