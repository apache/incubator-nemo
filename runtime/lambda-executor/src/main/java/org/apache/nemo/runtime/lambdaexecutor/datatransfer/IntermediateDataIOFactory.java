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

import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.runtime.executor.common.Serializer;

import java.util.Map;

/**
 */
public final class IntermediateDataIOFactory {
  private final PipeManagerWorker pipeManagerWorker;

  public IntermediateDataIOFactory(final PipeManagerWorker pipeManagerWorker) {
    this.pipeManagerWorker = pipeManagerWorker;
  }

  public PipeOutputWriter createPipeWriter(
    final int srcTaskIndex,
    final int originTaskindex,
    final RuntimeEdge<?> runtimeEdge,
    final Map<String, Serializer> serializerMap) {
    return new PipeOutputWriter(srcTaskIndex, originTaskindex, runtimeEdge, pipeManagerWorker, serializerMap);
  }
}
