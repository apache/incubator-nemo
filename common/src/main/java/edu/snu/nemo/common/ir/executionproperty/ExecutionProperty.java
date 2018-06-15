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
package edu.snu.nemo.common.ir.executionproperty;

import java.io.Serializable;
import java.util.Objects;

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

  /**
   * Static method to get an empty execution property.
   * @param <T> type of the value of the execution property.
   * @return an empty execution property.
   */
  static <T> ExecutionProperty<T> emptyExecutionProperty() {
    return new ExecutionProperty<T>(null, null) {
    };
  }

  @Override
  public final boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ExecutionProperty<?> that = (ExecutionProperty<?>) o;
    return getKey() == that.getKey()
        && Objects.equals(getValue(), that.getValue());
  }

  @Override
  public final int hashCode() {
    return Objects.hash(getKey(), getValue());
  }

  /**
   * Key for different types of execution property.
   */
  public enum Key {
    // Applies to IREdge
    DataCommunicationPattern,
    DataFlowModel,
    DataStore,
    MetricCollection,
    Partitioner,
    KeyExtractor,
    UsedDataHandling,
    Compression,
    DuplicateEdgeGroup,
    Coder,

    // Applies to IRVertex
    DynamicOptimizationType,
    ExecutorPlacement,
    Parallelism,
    ScheduleGroupIndex,
    StageId,
  }
}
