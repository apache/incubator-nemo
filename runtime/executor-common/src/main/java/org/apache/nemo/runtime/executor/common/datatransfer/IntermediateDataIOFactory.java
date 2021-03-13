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

import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.runtime.executor.common.DefaltIntermediateDataIOFactoryImpl;
import org.apache.nemo.runtime.executor.common.ExecutorThreadQueue;
import org.apache.nemo.runtime.executor.common.datatransfer.InputReader;
import org.apache.nemo.runtime.executor.common.datatransfer.OutputWriter;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.executor.common.SerializerManager;
import org.apache.reef.tang.annotations.DefaultImplementation;

import javax.inject.Inject;
import java.util.Optional;

/**
 * A factory that produces {@link InputReader} and {@link OutputWriter}.
 */
@DefaultImplementation(DefaltIntermediateDataIOFactoryImpl.class)
public interface IntermediateDataIOFactory {

  /**
   * Creates an {@link OutputWriter} between two stages.
   *
   * @param srcTaskId   the id of the source task.
   * @param runtimeEdge that connects the srcTask to the tasks belonging to dstIRVertex.
   * @return the {@link OutputWriter} created.
   */
  OutputWriter createWriter(final String srcTaskId,
                            final RuntimeEdge<?> runtimeEdge);

  OutputWriter createPipeWriter(
    final String srcTaskId,
    final RuntimeEdge<?> runtimeEdge,
    final TaskMetrics taskMetrics);

  /**
   * Creates an {@link InputReader} between two stages.
   *
   * @param dstTaskIdx  the index of the destination task.
   * @param srcIRVertex the {@link IRVertex} that output the data to be read.
   * @param runtimeEdge that connects the tasks belonging to srcIRVertex to dstTask.
   * @return the {@link InputReader} created.
   */
  InputReader createReader(final String taskId,
                           final IRVertex srcIRVertex,
                           final RuntimeEdge runtimeEdge,
                           final ExecutorThreadQueue executorThreadQueue);
}
