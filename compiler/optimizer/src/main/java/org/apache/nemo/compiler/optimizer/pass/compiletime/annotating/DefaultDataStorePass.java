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
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;

import java.util.List;

/**
 * Edge data store pass to process inter-stage memory store edges.
 */
@Annotates(DataStoreProperty.class)
public final class DefaultDataStorePass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public DefaultDataStorePass() {
    super(DefaultDataStorePass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    dag.getVertices().forEach(vertex -> { // Initialize the DataStore of the DAG with GlusterFileStore.
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      inEdges.forEach(edge ->
        edge.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.SerializedMemoryStore)));
    });
    return dag;
  }
}
