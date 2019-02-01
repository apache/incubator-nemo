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
package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.ir.IRDAG;
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
   *
   * @param dag          input DAG.
   * @param dagDirectory directory to save the DAG information.
   * @return optimized DAG, reshaped or tagged with execution properties.
   */
  IRDAG runCompileTimeOptimization(IRDAG dag, String dagDirectory);

  /**
   * Register runtime optimizations to the event handler.
   *
   * @param injector      Tang Injector which contains the implementations of run-time event handlers.
   * @param pubSubWrapper pub-sub event handler which managing run-time and compile-time event handling.
   */
  void registerRunTimeOptimizations(Injector injector, PubSubEventHandlerWrapper pubSubWrapper);
}
