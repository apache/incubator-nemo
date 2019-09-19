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
package org.apache.nemo.common.pass;

import org.apache.nemo.common.ir.IRDAG;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * Abstract class for optimization passes. All passes basically extends this class.
 */
public abstract class Pass implements Serializable {
  private Predicate<IRDAG> condition;

  /**
   * Default constructor.
   */
  public Pass() {
    this(dag -> true);
  }

  /**
   * Constructor.
   *
   * @param condition condition under which to run the pass.
   */
  private Pass(final Predicate<IRDAG> condition) {
    this.condition = condition;
  }

  /**
   * Getter for the condition under which to apply the pass.
   *
   * @return the condition under which to apply the pass.
   */
  public final Predicate<IRDAG> getCondition() {
    return this.condition;
  }

  /**
   * Add the condition to the existing condition to run the pass.
   *
   * @param newCondition the new condition to add to the existing condition.
   * @return the condition with the new condition added.
   */
  public final Pass addCondition(final Predicate<IRDAG> newCondition) {
    this.condition = this.condition.and(newCondition);
    return this;
  }
}
