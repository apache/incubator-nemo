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
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataPersistenceProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

/**
 * A pass to optimize large shuffle by tagging edges.
 * This pass handles the data persistence ExecutionProperty.
 */
@Annotates(DataPersistenceProperty.class)
@Requires(DataFlowProperty.class)
public final class LargeShuffleDataPersistencePass extends AnnotatingPass {

  /**
   * Default constructor.
   */
  public LargeShuffleDataPersistencePass() {
    super(LargeShuffleDataPersistencePass.class);
  }

  @Override
  public void optimize(final IRDAG dag) {
    dag.topologicalDo(irVertex ->
        dag.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
          final DataFlowProperty.Value dataFlowModel = irEdge.getPropertyValue(DataFlowProperty.class).get();
          if (DataFlowProperty.Value.Push.equals(dataFlowModel)) {
            irEdge.setPropertyPermanently(DataPersistenceProperty.of(DataPersistenceProperty.Value.Discard));
          }
        }));
  }
}
