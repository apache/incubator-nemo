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
package edu.snu.nemo.runtime.common.optimizer.pass.runtime;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.eventhandler.RuntimeEventHandler;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;

import java.io.Serializable;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Interface for dynamic optimization passes, for dynamically optimizing a physical plan.
 * It is a BiFunction that takes an original physical plan and metric data, to produce a new physical plan
 * after dynamic optimization.
 * @param <T> type of the metric data used for dynamic optimization.
 */
public interface RuntimePass<T> extends BiFunction<DAG<IRVertex, IREdge>, T, DAG<IRVertex, IREdge>>, Serializable {
  /**
   * @return the set of event handlers used with the runtime pass.
   */
  Set<Class<? extends RuntimeEventHandler>> getEventHandlerClasses();
}
