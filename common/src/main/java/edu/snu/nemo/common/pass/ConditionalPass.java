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
 * Abstract class for conditional passes. All passes basically extends this class.
 */
public abstract class ConditionalPass {
  private final PassCondition condition;

  /**
   * Default constructor.
   */
  public ConditionalPass() {
    this.condition = new PassCondition();
  }

  /**
   * Getter for the condition under which to apply the pass.
   * @return the condition under which to apply the pass.
   */
  public final PassCondition getCondition() {
    return this.condition;
  }

  /**
   * Add the condition to the existing condition to run the pass.
   * @param newCondition the new condition to add to the existing condition.
   * @return the condition with the new condition added.
   */
  public final Predicate<DAG<IRVertex, IREdge>> addCondition(final Predicate<DAG<IRVertex, IREdge>> newCondition) {
    // PassCondition adds the new condition to the existing one, with the 'and' boolean operator.
    return getCondition().and(newCondition);
  }
}
