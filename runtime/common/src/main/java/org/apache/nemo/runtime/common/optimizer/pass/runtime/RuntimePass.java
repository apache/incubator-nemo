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
package org.apache.nemo.runtime.common.optimizer.pass.runtime;

import org.apache.nemo.common.eventhandler.RuntimeEventHandler;
import org.apache.nemo.common.pass.Pass;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;

import java.util.Set;
import java.util.function.BiFunction;

/**
 * Abstract class for dynamic optimization passes, for dynamically optimizing a physical plan.
 * It is a BiFunction that takes an original physical plan and metric data, to produce a new physical plan
 * after dynamic optimization.
 * @param <T> type of the metric data used for dynamic optimization.
 */
public abstract class RuntimePass<T> extends Pass
    implements BiFunction<PhysicalPlan, T, PhysicalPlan> {
  /**
   * @return the set of event handlers used with the runtime pass.
   */
  public abstract Set<Class<? extends RuntimeEventHandler>> getEventHandlerClasses();
}
