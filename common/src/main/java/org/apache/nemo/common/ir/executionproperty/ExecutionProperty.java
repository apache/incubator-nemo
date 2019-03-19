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
package org.apache.nemo.common.ir.executionproperty;

import java.io.Serializable;

/**
 * An abstract class for each execution factors.
 * @param <T> Type of the value.
 */
public abstract class ExecutionProperty<T extends Serializable> implements Serializable {
  private T value;

  /**
   * Default constructor.
   * @param value value of the ExecutionProperty.
   */
  public ExecutionProperty(final T value) {
    this.value = value;
  }

  /**
   * @return the value of the execution property.
   */
  public final T getValue() {
    return this.value;
  }

  /**
   * @return the class of the value type.
   */
  public final Class<? extends Serializable> getValueClass() {
    return this.value.getClass();
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
    return value != null ? value.equals(that.value) : that.value == null;
  }

  @Override
  public final int hashCode() {
    return value != null ? value.hashCode() : 0;
  }

  @Override
  public final String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(this.getClass().getSimpleName());
    sb.append("(");
    sb.append(value.toString());
    sb.append(")");
    return sb.toString();
  }
}
