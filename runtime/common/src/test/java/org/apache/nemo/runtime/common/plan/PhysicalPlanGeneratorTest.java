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
package org.apache.nemo.runtime.common.plan;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ScheduleGroupProperty;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Test;

import java.util.Iterator;

import static org.apache.nemo.common.test.EmptyComponents.EMPTY_TRANSFORM;

/**
 * Tests {@link PhysicalPlanGenerator}.
 */
public final class PhysicalPlanGeneratorTest {

  @Test
  public void testBasic() throws Exception {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final PhysicalPlanGenerator physicalPlanGenerator = injector.getInstance(PhysicalPlanGenerator.class);

    final IRVertex v0 = newIRVertex(0, 5);
    final IRVertex v1 = newIRVertex(0, 3);
    final IRDAG irDAG = new IRDAG(new DAGBuilder<IRVertex, IREdge>()
      .addVertex(v0)
      .addVertex(v1)
      .connectVertices(newIREdge(v0, v1, CommunicationPatternProperty.Value.ONE_TO_ONE,
        DataFlowProperty.Value.PULL))
      .buildWithoutSourceSinkCheck());

    final DAG<Stage, StageEdge> stageDAG = physicalPlanGenerator.apply(irDAG);
    final Iterator<Stage> stages = stageDAG.getVertices().iterator();
    final Stage s0 = stages.next();
    final Stage s1 = stages.next();
  }

  private static final IRVertex newIRVertex(final int scheduleGroup, final int parallelism) {
    final IRVertex irVertex = new OperatorVertex(EMPTY_TRANSFORM);
    irVertex.setProperty(ScheduleGroupProperty.of(scheduleGroup));
    irVertex.setProperty(ParallelismProperty.of(parallelism));
    return irVertex;
  }

  private static final IREdge newIREdge(final IRVertex src, final IRVertex dst,
                                        final CommunicationPatternProperty.Value communicationPattern,
                                        final DataFlowProperty.Value dataFlowModel) {
    final IREdge irEdge = new IREdge(communicationPattern, src, dst);
    irEdge.setProperty(DataFlowProperty.of(dataFlowModel));
    return irEdge;
  }
}
