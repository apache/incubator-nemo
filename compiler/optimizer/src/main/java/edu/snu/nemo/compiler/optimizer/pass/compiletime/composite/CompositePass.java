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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.composite;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;

import java.util.*;

/**
 * A compile-time pass composed of multiple compile-time passes, which each modifies an IR DAG.
 */
public abstract class CompositePass implements CompileTimePass {
  private final List<CompileTimePass> passList;
  private final Set<Class<? extends ExecutionProperty>> prerequisiteExecutionProperties;

  /**
   * Constructor.
   * @param passList list of compile time passes.
   */
  public CompositePass(final List<CompileTimePass> passList) {
    this.passList = passList;
    this.prerequisiteExecutionProperties = new HashSet<>();
    passList.forEach(pass -> prerequisiteExecutionProperties.addAll(pass.getPrerequisiteExecutionProperties()));
    passList.forEach(pass -> {
      if (pass instanceof AnnotatingPass) {
        prerequisiteExecutionProperties.remove(((AnnotatingPass) pass).getExecutionPropertyToModify());
      }
    });
  }

  /**
   * Getter for list of compile time passes.
   * @return the list of CompileTimePass.
   */
  public final List<CompileTimePass> getPassList() {
    return passList;
  }

  @Override
  public final DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> irVertexIREdgeDAG) {
    return recursivelyApply(irVertexIREdgeDAG, getPassList().iterator());
  }

  /**
   * Recursively apply the give list of passes.
   * @param dag dag.
   * @param passIterator pass iterator.
   * @return dag.
   */
  private DAG<IRVertex, IREdge> recursivelyApply(final DAG<IRVertex, IREdge> dag,
                                                 final Iterator<CompileTimePass> passIterator) {
    if (passIterator.hasNext()) {
      return recursivelyApply(passIterator.next().apply(dag), passIterator);
    } else {
      return dag;
    }
  }

  @Override
  public final Set<Class<? extends ExecutionProperty>> getPrerequisiteExecutionProperties() {
    return prerequisiteExecutionProperties;
  }
}
