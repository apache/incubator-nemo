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

import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.offloading.common.ServerlessExecutorProvider;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;

import java.io.Serializable;
import java.util.Optional;

/**
 * Transform Context Implementation.
 */
public final class TransformContextImpl implements Transform.Context {
  private String data;
  private final IRVertex irVertex;
  private final ServerlessExecutorProvider serverlessExecutorProvider;
  private final String taskId;
  private final StateStore stateStore;

  /**
   * Constructor of Context Implementation.
   */
  public TransformContextImpl(final IRVertex irVertex,
                              final ServerlessExecutorProvider serverlessExecutorProvider,
                              final String taskId,
                              final StateStore stateStore) {
    this.data = null;
    this.irVertex = irVertex;
    this.serverlessExecutorProvider = serverlessExecutorProvider;
    this.taskId = taskId;
    this.stateStore = stateStore;
  }

  @Override
  public StateStore getStateStore() {
    return stateStore;
  }

  @Override
  public String getTaskId() {
    return taskId;
  }

  @Override
  public ServerlessExecutorProvider getServerlessExecutorProvider() {
    return serverlessExecutorProvider;
  }

  @Override
  public Object getBroadcastVariable(final Serializable tag) {
    throw new RuntimeException("not supported");
  }

  @Override
  public void setSerializedData(final String serializedData) {
    this.data = serializedData;
  }

  @Override
  public Optional<String> getSerializedData() {
    return Optional.ofNullable(this.data);
  }

  @Override
  public IRVertex getIRVertex() {
    return irVertex;
  }
}
