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
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.test.EmptyComponents;
import org.apache.nemo.compiler.optimizer.policy.BasicPullPolicy;
import org.apache.nemo.compiler.optimizer.policy.BasicPushPolicy;
import org.apache.nemo.compiler.optimizer.policy.Policy;
import org.apache.nemo.conf.JobConf;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

/**
 * Generates physical plans for testing purposes.
 */
public final class TestPlanGenerator {
  private static final PhysicalPlanGenerator PLAN_GENERATOR;
  private static final String EMPTY_DAG_DIRECTORY = "";

  static {
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(JobConf.DAGDirectory.class, EMPTY_DAG_DIRECTORY);
    try {
      PLAN_GENERATOR = injector.getInstance(PhysicalPlanGenerator.class);
    } catch (InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Type of the plan to generate.
   */
  public enum PlanType {
    TwoVerticesJoined,
    ThreeSequentialVertices,
    ThreeSequentialVerticesWithDifferentContainerTypes
  }

  /**
   * private constructor.
   */
  private TestPlanGenerator() {
  }

  /**
   * @param planType type of the plan to generate.
   * @param isPush   whether to use the push policy.
   * @return the generated plan.
   * @throws Exception exception.
   */
  public static PhysicalPlan generatePhysicalPlan(final PlanType planType, final boolean isPush) throws Exception {
    final Policy policy = isPush ? new BasicPushPolicy() : new BasicPullPolicy();
    switch (planType) {
      case TwoVerticesJoined:
        return convertIRToPhysical(getTwoVerticesJoinedDAG(), policy);
      case ThreeSequentialVertices:
        return convertIRToPhysical(getThreeSequentialVerticesDAG(true), policy);
      case ThreeSequentialVerticesWithDifferentContainerTypes:
        return convertIRToPhysical(getThreeSequentialVerticesDAG(false), policy);
      default:
        throw new IllegalArgumentException(planType.toString());
    }
  }

  /**
   * @param irDAG  irDAG.
   * @param policy policy.
   * @return convert an IR into a physical plan using the given policy.
   * @throws Exception exception.
   */
  private static PhysicalPlan convertIRToPhysical(final IRDAG irDAG,
                                                  final Policy policy) {
    final IRDAG optimized = policy.runCompileTimeOptimization(irDAG, EMPTY_DAG_DIRECTORY);
    final DAG<Stage, StageEdge> physicalDAG = PLAN_GENERATOR.apply(optimized);
    return new PhysicalPlan("TestPlan", physicalDAG);
  }

  /**
   * @return a dag that joins two vertices.
   */
  private static IRDAG getTwoVerticesJoinedDAG() {
    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();

    final Transform t = new EmptyComponents.EmptyTransform("empty");
    final IRVertex v1 = new OperatorVertex(t);
    v1.setProperty(ParallelismProperty.of(3));
    v1.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));
    dagBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setProperty(ParallelismProperty.of(2));
    v2.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));
    dagBuilder.addVertex(v2);

    final IRVertex v3 = new OperatorVertex(t);
    v3.setProperty(ParallelismProperty.of(3));
    v3.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));
    dagBuilder.addVertex(v3);

    final IRVertex v4 = new OperatorVertex(t);
    v4.setProperty(ParallelismProperty.of(2));
    v4.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));
    dagBuilder.addVertex(v4);

    final IRVertex v5 = new OperatorVertex(t);
    v5.setProperty(ParallelismProperty.of(2));
    v5.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));
    dagBuilder.addVertex(v5);

    final IREdge e1 = EmptyComponents.newDummyShuffleEdge(v1, v2);
    dagBuilder.connectVertices(e1);

    final IREdge e2 = EmptyComponents.newDummyShuffleEdge(v3, v2);
    dagBuilder.connectVertices(e2);

    final IREdge e3 = EmptyComponents.newDummyShuffleEdge(v2, v4);
    dagBuilder.connectVertices(e3);

    final IREdge e4 = EmptyComponents.newDummyShuffleEdge(v4, v5);
    dagBuilder.connectVertices(e4);

    return new IRDAG(dagBuilder.buildWithoutSourceSinkCheck());
  }

  /**
   * @param sameContainerType whether all three vertices are of the same container type
   * @return a dag with 3 sequential vertices.
   */
  private static IRDAG getThreeSequentialVerticesDAG(final boolean sameContainerType) {
    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();

    final Transform t = new EmptyComponents.EmptyTransform("empty");
    final IRVertex v1 = new OperatorVertex(t);
    v1.setProperty(ParallelismProperty.of(3));
    v1.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));
    dagBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setProperty(ParallelismProperty.of(2));
    if (sameContainerType) {
      v2.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));
    } else {
      v2.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.TRANSIENT));
    }
    dagBuilder.addVertex(v2);

    final IRVertex v3 = new OperatorVertex(t);
    v3.setProperty(ParallelismProperty.of(2));
    if (sameContainerType) {
      v3.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));
    } else {
      v3.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.TRANSIENT));
    }
    dagBuilder.addVertex(v3);

    final IREdge e1 = EmptyComponents.newDummyShuffleEdge(v1, v2);
    dagBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(CommunicationPatternProperty.Value.ONE_TO_ONE, v2, v3);
    dagBuilder.connectVertices(e2);

    return new IRDAG(dagBuilder.buildWithoutSourceSinkCheck());
  }
}
