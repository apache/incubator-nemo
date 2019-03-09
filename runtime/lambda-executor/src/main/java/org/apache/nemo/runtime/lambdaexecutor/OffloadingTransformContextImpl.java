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
package org.apache.nemo.runtime.lambdaexecutor;

import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.offloading.common.ServerlessExecutorProvider;

import java.io.Serializable;
import java.util.Optional;

/**
 * Transform Context Implementation.
 */
public final class OffloadingTransformContextImpl implements Transform.Context {
  private final IRVertex irVertex;

  /**
   * Constructor of Context Implementation.
   */
  public OffloadingTransformContextImpl(final IRVertex irVertex) {
    this.irVertex = irVertex;
  }

  @Override
  public ServerlessExecutorProvider getServerlessExecutorProvider() {
    return null;
  }

  @Override
  public Object getBroadcastVariable(final Serializable tag) {
    return null;
  }

  @Override
  public void setSerializedData(final String serializedData) {
  }

  @Override
  public Optional<String> getSerializedData() {
    return Optional.ofNullable(null);
  }

  @Override
  public IRVertex getIRVertex() {
    return irVertex;
  }
}
