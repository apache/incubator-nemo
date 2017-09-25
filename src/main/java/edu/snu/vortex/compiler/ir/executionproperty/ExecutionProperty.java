/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.vortex.compiler.ir.executionproperty;

import java.io.Serializable;

/**
 * An abstract class for each execution factors.
 * @param <T> Key of the value.
 */
public abstract class ExecutionProperty<T> implements Serializable {
  private Key key;
  private T value;

  /**
   * Default constructor.
   * @param key key of the ExecutionProperty, given by the enum in this class.
   * @param value value of the ExecutionProperty.
   */
  public ExecutionProperty(final Key key, final T value) {
    this.key = key;
    this.value = value;
  }

  /**
   * @return the value of the execution property.
   */
  public final T getValue() {
    return this.value;
  }

  /**
   * @return the key of the execution property.
   */
  public final Key getKey() {
    return key;
  }

  static <T> ExecutionProperty<T> emptyExecutionProperty() {
    return new ExecutionProperty<T>(null, null) {
    };
  }

  /**
   * Key for different types of execution property.
   */
  public enum Key {
    // Applies to IREdge
    DataCommunicationPattern, // TODO #492: modularizing runtime components for data communication pattern.
    DataFlowModel,
    DataStore,
    MetricCollection,
    IsSideInput, // TODO #508: re-investigate sideInputs.
    Partitioning, // TODO #493: modularizing runtime components for partitioning.
    WriteOptimization, // TODO #492: to be removed.

    // Applies to IRVertex
    DynamicOptimizationType,
    ExecutorPlacement,
    Parallelism,
    ScheduleGroupIndex,
    StageId,

    // Applies to entire job DAG
    SchedulingPolicy,
    SchedulerType,
  }
}
