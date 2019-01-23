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
package org.apache.nemo.compiler.optimizer.pass.runtime;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.pass.Pass;

/**
 * Abstract class for dynamic optimization passes, for dynamically optimizing the IRDAG.
 * @param <T> type of the message used for dynamic optimization.
 */
public abstract class RunTimePass<T> extends Pass {
  /**
   * @param irdag to optimize.
   * @param message dynamically generated during the execution.
   * @return optimized DAG.
   */
  public abstract IRDAG optimize(final IRDAG irdag, final T message);
}
