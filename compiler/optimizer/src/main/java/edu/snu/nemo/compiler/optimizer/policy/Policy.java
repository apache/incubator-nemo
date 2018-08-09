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
package edu.snu.nemo.compiler.optimizer.policy;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import org.apache.reef.tang.Injector;

import java.io.Serializable;

/**
 * An interface for policies, each of which is composed of a list of static optimization passes.
 * The list of static optimization passes are run in the order provided by the implementation.
 * Most policies follow the implementation in {@link PolicyImpl}.
 */
public interface Policy extends Serializable {
  /**
   * Optimize the DAG with the compile time optimizations.
   * @param dag input DAG.
   * @param dagDirectory directory to save the DAG information.
   * @return optimized DAG, reshaped or tagged with execution properties.
   * @throws Exception throws an exception if there is an exception.
   */
  DAG<IRVertex, IREdge> runCompileTimeOptimization(DAG<IRVertex, IREdge> dag, String dagDirectory) throws Exception;

  /**
   * Register runtime optimizations to the event handler.
   * @param injector Tang Injector, used in the UserApplicationRunner.
   * @param pubSubWrapper pub-sub event handler, used in the UserApplicationRunner.
   */
  void registerRunTimeOptimizations(Injector injector, PubSubEventHandlerWrapper pubSubWrapper);
}
