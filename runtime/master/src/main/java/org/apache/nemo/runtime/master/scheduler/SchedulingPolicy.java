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
package org.apache.nemo.runtime.master.scheduler;

import net.jcip.annotations.ThreadSafe;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Collection;

/**
 * A function to select an executor from collection of available executors.
 */
@DriverSide
@ThreadSafe
@FunctionalInterface
@DefaultImplementation(MinOccupancyFirstSchedulingPolicy.class)
public interface SchedulingPolicy {
  /**
   * A function to select an executor from the specified collection of available executors.
   *
   * @param executors The collection of available executors.
   *                  Implementations can assume that the collection is not empty.
   * @param task      The task to schedule
   * @return The selected executor. It must be a member of {@code executors}.
   */
  ExecutorRepresenter selectExecutor(Collection<ExecutorRepresenter> executors, Task task);
}
