/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.vortex.compiler.ir;

import edu.snu.vortex.common.dag.DAG;

import java.util.*;

/**
 * IRVertex that collects statistics to send them to the optimizer for dynamic optimization.
 * This class is generated in the DAG through {@link edu.snu.vortex.compiler.optimizer.passes.DataSkewPass}.
 */
public final class MetricCollectionBarrierVertex extends IRVertex {
  // Partition ID to Size data
  private final Map<String, Long> metricData;
  // This DAG snapshot is taken at the end of the DataSkewPass, for the vertex to know the state of the DAG at its
  // optimization, and to be able to figure out exactly where in the DAG the vertex exists.
  private DAG<IRVertex, IREdge> dagSnapshot;

  /**
   * Constructor for dynamic optimization vertex.
   */
  public MetricCollectionBarrierVertex() {
    this.metricData = new HashMap<>();
    this.dagSnapshot = null;
  }

  @Override
  public MetricCollectionBarrierVertex getClone() {
    final MetricCollectionBarrierVertex that = new MetricCollectionBarrierVertex();
    that.setDAGSnapshot(dagSnapshot);
    IRVertex.copyAttributes(this, that);
    return that;
  }

  /**
   * This is to set the DAG snapshot at the end of the DataSkewPass.
   * @param dag DAG to set on the vertex.
   */
  public void setDAGSnapshot(final DAG<IRVertex, IREdge> dag) {
    this.dagSnapshot = dag;
  }

  /**
   * Access the DAG snapshot when triggering dynamic optimization.
   * @return the DAG set to the vertex, or throws an exception otherwise.
   */
  private DAG<IRVertex, IREdge> getDAGSnapshot() {
    if (this.dagSnapshot == null) {
      throw new RuntimeException("MetricCollectionBarrierVertex must have been set with a DAG.");
    }
    return this.dagSnapshot;
  }

  /**
   * Method for accumulating metrics in the vertex.
   * @param partitionID key, or ID of the partition.
   * @param partitionSize value of the size of the partition data.
   */
  public void accumulateMetrics(final String partitionID, final Long partitionSize) {
    metricData.putIfAbsent(partitionID, partitionSize);
  }

  /**
   * Method for triggering dynamic optimization.
   * It can be accessed by the Runtime Master, and it will trigger dynamic optimization through this method.
   */
  public void triggerDynamicOptimization() {
    // TODO #315: resubmitting DAG to runtime.
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(irVertexPropertiesToString());
    sb.append("}");
    return sb.toString();
  }
}
