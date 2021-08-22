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
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.EnableWorkStealingProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.WorkStealingSubSplitProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.apache.nemo.runtime.common.plan.StagePartitioner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Optimization pass for tagging work stealing sub-split execution property.
 */
@Annotates(WorkStealingSubSplitProperty.class)
@Requires({EnableWorkStealingProperty.class, ParallelismProperty.class})
public final class WorkStealingSubSplitPass extends AnnotatingPass {
  private static final String SPLIT_STRATEGY = "SPLIT";
  private static final String MERGE_STRATEGY = "MERGE";
  private static final String DEFAULT_STRATEGY = "DEFAULT";

  private static final int MAX_SUB_SPLIT_NUM = 10;
  private static final int DEFAULT_SUB_SPLIT_NUM = 1;

  private final StagePartitioner stagePartitioner = new StagePartitioner();
  /**
   * Default Constructor.
   */
  public WorkStealingSubSplitPass() {
    super(WorkStealingSubSplitPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG irdag) {
    final Map<IRVertex, Integer> vertexToSplitNum = new HashMap<>();

    for (IRVertex vertex : irdag.getTopologicalSort()) {
      if (vertex.getPropertyValue(EnableWorkStealingProperty.class)
        .orElse(DEFAULT_STRATEGY).equals(SPLIT_STRATEGY)) {
        int maxSourceParallelism = irdag.getIncomingEdgesOf(vertex).stream().map(IREdge::getSrc)
          .mapToInt(v -> v.getPropertyValue(ParallelismProperty.class).orElse(DEFAULT_SUB_SPLIT_NUM))
          .max().orElse(DEFAULT_SUB_SPLIT_NUM);
        if (maxSourceParallelism > MAX_SUB_SPLIT_NUM) {
          vertex.setProperty(WorkStealingSubSplitProperty.of(DEFAULT_SUB_SPLIT_NUM));
        } else {
          vertex.setProperty(WorkStealingSubSplitProperty.of(maxSourceParallelism));
          vertexToSplitNum.put(vertex, maxSourceParallelism);
        }
      } else {
        vertex.setProperty(WorkStealingSubSplitProperty.of(DEFAULT_SUB_SPLIT_NUM));
      }
    }

    updateParallelismProperty(irdag, vertexToSplitNum);
    return irdag;
  }

  private void updateParallelismProperty(IRDAG irdag, Map<IRVertex, Integer> vertexToSplitNum) {
    final Map<IRVertex, Integer> vertexToStageId = stagePartitioner.apply(irdag);

    final Map<Integer, Set<IRVertex>> stageIdToStageVertices = new HashMap<>();
    vertexToStageId.forEach((vertex, stageId) -> {
      if (!stageIdToStageVertices.containsKey(stageId)) {
        stageIdToStageVertices.put(stageId, new HashSet<>());
      }
      stageIdToStageVertices.get(stageId).add(vertex);
    });

    for (IRVertex vertex : vertexToSplitNum.keySet()) {
      int numSubSplit = vertexToSplitNum.get(vertex);
      for (IRVertex stageVertex : stageIdToStageVertices.get(vertexToStageId.get(vertex))) {
        int currentParallelism = stageVertex.getPropertyValue(ParallelismProperty.class).get();
        stageVertex.setProperty(ParallelismProperty.of(currentParallelism * numSubSplit));
      }
    }
  }
}
