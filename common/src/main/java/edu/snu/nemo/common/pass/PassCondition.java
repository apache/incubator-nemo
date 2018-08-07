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

package edu.snu.nemo.common.pass;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;

import java.util.function.Predicate;

/**
 * Wrapper for {@link Predicate} for which we run the given pass.
 */
public final class PassCondition implements Predicate<DAG<IRVertex, IREdge>> {
  private Predicate<DAG<IRVertex, IREdge>> condition;

  /**
   * Default constructor.
   */
  public PassCondition() {
    this.condition = (dag) -> true;
  }

  @Override
  public boolean test(final DAG<IRVertex, IREdge> dag) {
    return condition.test(dag);
  }

  @Override
  public Predicate<DAG<IRVertex, IREdge>> and(final Predicate<? super DAG<IRVertex, IREdge>> newCondition) {
    this.condition = this.condition.and(newCondition);
    return this.condition;
  }

  @Override
  public Predicate<DAG<IRVertex, IREdge>> negate() {
    this.condition = this.condition.negate();
    return this.condition;
  }

  @Override
  public Predicate<DAG<IRVertex, IREdge>> or(final Predicate<? super DAG<IRVertex, IREdge>> newCondition) {
    this.condition = this.condition.or(newCondition);
    return this.condition;
  }
}
