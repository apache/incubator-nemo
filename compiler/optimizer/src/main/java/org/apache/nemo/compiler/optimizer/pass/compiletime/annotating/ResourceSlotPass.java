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
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;

/**
 * Sets {@link ResourceSlotProperty}.
 */
@Annotates(ResourceSlotProperty.class)
public final class ResourceSlotPass extends AnnotatingPass {

  /**
   * Constructor.
   */
  public ResourceSlotPass() {
    super(ResourceSlotPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    // On every vertex, if ResourceSlotProperty is not set, put it as true.
    dag.getVertices().stream()
        .filter(v -> !v.getPropertyValue(ResourceSlotProperty.class).isPresent())
        .forEach(v -> v.setProperty(ResourceSlotProperty.of(true)));
    return dag;
  }
}
