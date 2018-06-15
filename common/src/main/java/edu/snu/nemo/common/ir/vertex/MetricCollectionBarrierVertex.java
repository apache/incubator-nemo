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

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.exception.DynamicOptimizationException;
import edu.snu.nemo.common.ir.edge.IREdge;

import java.util.*;

/**
 * IRVertex that collects statistics to send them to the optimizer for dynamic optimization.
 * This class is generated in the DAG through
 * {edu.snu.nemo.compiler.optimizer.pass.compiletime.composite.DataSkewCompositePass}.
 * @param <K> type of the key of metric data.
 * @param <V> type of the value of metric data.
 */
public final class MetricCollectionBarrierVertex<K, V> extends IRVertex {
  // Metric data used for dynamic optimization.
  private Map<K, V> metricData;
  private final List<String> blockIds;

  // This DAG snapshot is taken at the end of the DataSkewCompositePass, for the vertex to know the state of the DAG at
  // its optimization, and to be able to figure out exactly where in the DAG the vertex exists.
  private DAG<IRVertex, IREdge> dagSnapshot;

  /**
   * Constructor for dynamic optimization vertex.
   */
  public MetricCollectionBarrierVertex() {
    this.metricData = null;
    this.blockIds = new ArrayList<>();
    this.dagSnapshot = null;
  }

  @Override
  public MetricCollectionBarrierVertex getClone() {
    final MetricCollectionBarrierVertex that = new MetricCollectionBarrierVertex();
    that.setDAGSnapshot(dagSnapshot);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  /**
   * This is to set the DAG snapshot at the end of the DataSkewCompositePass.
   * @param dag DAG to set on the vertex.
   */
  public void setDAGSnapshot(final DAG<IRVertex, IREdge> dag) {
    this.dagSnapshot = dag;
  }

  /**
   * Access the DAG snapshot when triggering dynamic optimization.
   * @return the DAG set to the vertex, or throws an exception otherwise.
   */
  public DAG<IRVertex, IREdge> getDAGSnapshot() {
    if (this.dagSnapshot == null) {
      throw new DynamicOptimizationException("MetricCollectionBarrierVertex must have been set with a DAG.");
    }
    return this.dagSnapshot;
  }

  /**
   * Method for accumulating metrics in the vertex.
   * @param metric map of hash value of the key of the block to the block size.
   */
  public void setMetricData(final Map<K, V> metric) {
    metricData = metric;
  }

  /**
   * Method for retrieving metrics from the vertex.
   * @return the accumulated metric data.
   */
  public Map<K, V> getMetricData() {
    return metricData;
  }

  /**
   * Add block id that is needed for optimization in RuntimePass.
   * @param blockId the block id subjected to the optimization.
   */
  public void addBlockId(final String blockId) {
    blockIds.add(blockId);
  }

  /**
   * Retrieve block ids.
   * @return the block ids subjected to optimization.
   */
  public List<String> getBlockIds() {
    return blockIds;
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
