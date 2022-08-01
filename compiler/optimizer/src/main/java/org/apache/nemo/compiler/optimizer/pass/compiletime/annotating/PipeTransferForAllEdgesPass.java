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
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;

/**
 * Annotate 'Pipe' on all edges.
 */
@Annotates(DataStoreProperty.class)
public final class PipeTransferForAllEdgesPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public PipeTransferForAllEdgesPass() {
    super(PipeTransferForAllEdgesPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    dag.getVertices().forEach(vertex ->
      dag.getIncomingEdgesOf(vertex).forEach(edge -> {
        edge.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.PIPE));
        edge.setPropertyPermanently(DataFlowProperty.of(DataFlowProperty.Value.PUSH));
        edge.setPropertyPermanently(DataPersistenceProperty.of(DataPersistenceProperty.Value.DISCARD));
      }));
    return dag;
  }
}
