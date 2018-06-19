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
}
