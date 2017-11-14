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
package edu.snu.onyx.runtime.master;

import edu.snu.onyx.client.JobConf;
import edu.snu.onyx.common.PubSubEventHandlerWrapper;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.compiler.backend.Backend;
import edu.snu.onyx.compiler.backend.onyx.OnyxBackend;
import edu.snu.onyx.compiler.eventhandler.DynamicOptimizationEventHandler;
import edu.snu.onyx.compiler.ir.IREdge;
import edu.snu.onyx.compiler.ir.IRVertex;
import edu.snu.onyx.compiler.optimizer.Optimizer;
import edu.snu.onyx.compiler.optimizer.policy.Policy;
import edu.snu.onyx.runtime.common.plan.physical.PhysicalPlan;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.SerializationUtils;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Base64;

/**
 * Compiles and runs User application.
 */
public final class UserApplicationRunner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(UserApplicationRunner.class.getName());

  private final String dagDirectory;
  private final String dagString;
  private final String optimizationPolicyCanonicalName;
  private final int maxScheduleAttempt;

  private final RuntimeMaster runtimeMaster;
  private final Backend<PhysicalPlan> backend;

  @Inject
  private UserApplicationRunner(@Parameter(JobConf.DAGDirectory.class) final String dagDirectory,
                                @Parameter(JobConf.SerializedDAG.class) final String dagString,
                                @Parameter(JobConf.OptimizationPolicy.class) final String optimizationPolicy,
                                @Parameter(JobConf.MaxScheduleAttempt.class) final int maxScheduleAttempt,
                                final PubSubEventHandlerWrapper pubSubEventHandlerWrapper,
                                final DynamicOptimizationEventHandler dynamicOptimizationEventHandler,
                                final RuntimeMaster runtimeMaster) {
    this.dagDirectory = dagDirectory;
    this.dagString = dagString;
    this.optimizationPolicyCanonicalName = optimizationPolicy;
    this.maxScheduleAttempt = maxScheduleAttempt;
    this.runtimeMaster = runtimeMaster;
    this.backend = new OnyxBackend();
    pubSubEventHandlerWrapper.getPubSubEventHandler()
        .subscribe(dynamicOptimizationEventHandler.getEventClass(), dynamicOptimizationEventHandler);
  }

  @Override
  public void run() {
    try {
      LOG.info("##### ONYX Compiler #####");

      final DAG<IRVertex, IREdge> dag = SerializationUtils.deserialize(Base64.getDecoder().decode(dagString));
      dag.storeJSON(dagDirectory, "ir", "IR before optimization");
      final Policy optimizationPolicy = (Policy) Class.forName(optimizationPolicyCanonicalName).newInstance();

      final DAG<IRVertex, IREdge> optimizedDAG = Optimizer.optimize(dag, optimizationPolicy, dagDirectory);
      optimizedDAG.storeJSON(dagDirectory, "ir-" + optimizationPolicy.getClass().getSimpleName(),
          "IR optimized for " + optimizationPolicy.getClass().getSimpleName());

      final PhysicalPlan physicalPlan = backend.compile(optimizedDAG);

      physicalPlan.getStageDAG().storeJSON(dagDirectory, "plan", "physical execution plan by compiler");
      runtimeMaster.execute(physicalPlan, maxScheduleAttempt);
      runtimeMaster.terminate();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
