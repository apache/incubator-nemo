/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.common.ir.vertex;

import edu.snu.nemo.common.KeyExtractor;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.exception.DynamicOptimizationException;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.transform.MetricCollectTransform;
import edu.snu.nemo.common.ir.vertex.transform.Transform;

/**
 * IRVertex that collects statistics to send them to the optimizer for dynamic optimization.
 * This class is generated in the DAG through
 * {edu.snu.nemo.compiler.optimizer.pass.compiletime.composite.DataSkewCompositePass}.
 */
public class MetricCollectionVertex extends IRVertex {
  // This DAG snapshot is taken at the end of the DataSkewCompositePass, for the vertex to know the state of the DAG at
  // its optimization, and to be able to figure out exactly where in the DAG the vertex exists.
  private final Transform transform;
  private final String dstVertexId;
  private final KeyExtractor keyExtractor;
  private DAG<IRVertex, IREdge> dagSnapshot;

  /**
   * Constructor of MetricCollectionVertex.
   */
  public MetricCollectionVertex(final String dstVertexId, final KeyExtractor keyExtractor) {
    super();
    this.dstVertexId = dstVertexId;
    this.transform = new MetricCollectTransform(dstVertexId, keyExtractor);
    this.keyExtractor = keyExtractor;
  }

  /**
   * @return the transform in the OperatorVertex.
   */
  public final Transform getTransform() {
    return transform;
  }

  @Override
  public final MetricCollectionVertex getClone() {
    final MetricCollectionVertex that = new MetricCollectionVertex(dstVertexId, keyExtractor);
    that.setDAGSnapshot(dagSnapshot);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  /**
   * This is to set the DAG snapshot at the end of the DataSkewCompositePass.
   * @param dag DAG to set on the vertex.
   */
  public final void setDAGSnapshot(final DAG<IRVertex, IREdge> dag) {
    this.dagSnapshot = dag;
  }

  /**
   * Access the DAG snapshot when triggering dynamic optimization.
   * @return the DAG set to the vertex, or throws an exception otherwise.
   */
  public final DAG<IRVertex, IREdge> getDAGSnapshot() {
    if (this.dagSnapshot == null) {
      throw new DynamicOptimizationException("MetricCollectionVertex must have been set with a DAG.");
    }
    return this.dagSnapshot;
  }

  @Override
  public final String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(irVertexPropertiesToString());
    sb.append(", \"transform\": \"");
    sb.append(transform);
    sb.append("\"}");
    return sb.toString();
  }
}
