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

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ClonedSchedulingProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

/**
 * Set the ClonedScheduling property of source vertices, in an upfront manner.
 */
@Annotates(ClonedSchedulingProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class UpfrontCloningPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public UpfrontCloningPass() {
    super(UpfrontCloningPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    dag.getVertices().stream()
      .filter(vertex -> dag.getIncomingEdgesOf(vertex.getId())
        .stream()
        // TODO #198: Handle Un-cloneable Beam Sink Operators
        // only shuffle receivers (for now... as particular Beam sink operators fail when cloned)
        .anyMatch(edge ->
          edge.getPropertyValue(CommunicationPatternProperty.class)
            .orElseThrow(() -> new IllegalStateException())
            .equals(CommunicationPatternProperty.Value.SHUFFLE))
      )
      .forEach(vertex -> vertex.setProperty(
        ClonedSchedulingProperty.of(new ClonedSchedulingProperty.CloneConf()))); // clone upfront, always
    return dag;
  }
}
