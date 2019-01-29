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
package org.apache.nemo.compiler.optimizer;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.compiler.optimizer.pass.runtime.Message;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * An interface for optimizer, which manages the optimization over submitted IR DAGs through
 * {@link org.apache.nemo.compiler.optimizer.policy.Policy}s.
 */
@DefaultImplementation(NemoOptimizer.class)
public interface Optimizer {

  /**
   * Optimize the submitted DAG at compile time.
   *
   * @param dag the input DAG to optimize.
   * @return optimized DAG, reshaped or tagged with execution properties.
   */
  IRDAG optimizeAtCompileTime(IRDAG dag);

  /**
   * Optimize the submitted DAG at run time.
   *
   * @param dag input.
   * @param message for optimization.
   * @return optimized DAG.
   */
  IRDAG optimizeAtRunTime(final IRDAG dag, final Message message);
}
