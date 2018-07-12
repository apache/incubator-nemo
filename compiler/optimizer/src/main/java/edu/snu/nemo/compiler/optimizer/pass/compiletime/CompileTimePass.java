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
package edu.snu.nemo.compiler.optimizer.pass.compiletime;

import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;

import java.io.Serializable;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Interface for compile-time optimization passes that processes the DAG.
 * It is a function that takes an original DAG to produce a processed DAG, after an optimization.
 */
public interface CompileTimePass extends Function<DAG<IRVertex, IREdge>, DAG<IRVertex, IREdge>>, Serializable {
  /**
   * Getter for prerequisite execution properties.
   * @return set of prerequisite execution properties.
   */
  Set<Class<? extends ExecutionProperty>> getPrerequisiteExecutionProperties();

  /**
   * Getter for the condition under which to apply the pass.
   * @return the condition under which to apply the pass.
   */
  PassCondition getCondition();

  /**
   * Add the condition to the condition to run the pass.
   * @param newCondition the new condition to add to the existing condition.
   * @return the condition with the new condition added.
   */
  default Predicate<DAG<IRVertex, IREdge>> addCondition(Predicate<DAG<IRVertex, IREdge>> newCondition) {
    return getCondition().and(newCondition);
  }
}
