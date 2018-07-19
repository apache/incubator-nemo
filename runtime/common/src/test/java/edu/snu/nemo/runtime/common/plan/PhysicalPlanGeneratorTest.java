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
package edu.snu.nemo.runtime.common.plan;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataFlowModelProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ScheduleGroupProperty;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Test;

import java.util.Iterator;

import static edu.snu.nemo.common.test.EmptyComponents.EMPTY_TRANSFORM;
import static org.junit.Assert.assertNotEquals;

/**
 * Tests {@link PhysicalPlanGenerator}.
 */
public final class PhysicalPlanGeneratorTest {

  /**
   * Test splitting ScheduleGroups by Pull StageEdges.
   * @throws Exception exceptions on the way
   */
  @Test
  public void testSplitScheduleGroupByPullStageEdges() throws Exception {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final PhysicalPlanGenerator physicalPlanGenerator = injector.getInstance(PhysicalPlanGenerator.class);

    final IRVertex v0 = newIRVertex(0, 5);
    final IRVertex v1 = newIRVertex(0, 3);
    final DAG<IRVertex, IREdge> irDAG = new DAGBuilder<IRVertex, IREdge>()
        .addVertex(v0)
        .addVertex(v1)
        .connectVertices(newIREdge(v0, v1, DataCommunicationPatternProperty.Value.OneToOne,
            DataFlowModelProperty.Value.Pull))
        .buildWithoutSourceSinkCheck();

    final DAG<Stage, StageEdge> stageDAG = physicalPlanGenerator.apply(irDAG);
    final Iterator<Stage> stages = stageDAG.getVertices().iterator();
    final Stage s0 = stages.next();
    final Stage s1 = stages.next();

    assertNotEquals(s0.getScheduleGroup(), s1.getScheduleGroup());
  }

  private static final IRVertex newIRVertex(final int scheduleGroup, final int parallelism) {
    final IRVertex irVertex = new OperatorVertex(EMPTY_TRANSFORM);
    irVertex.getExecutionProperties().put(ScheduleGroupProperty.of(scheduleGroup));
    irVertex.getExecutionProperties().put(ParallelismProperty.of(parallelism));
    return irVertex;
  }

  private static final IREdge newIREdge(final IRVertex src, final IRVertex dst,
                                        final DataCommunicationPatternProperty.Value communicationPattern,
                                        final DataFlowModelProperty.Value dataFlowModel) {
    final IREdge irEdge = new IREdge(communicationPattern, src, dst);
    irEdge.getExecutionProperties().put(DataFlowModelProperty.of(dataFlowModel));
    return irEdge;
  }
}
