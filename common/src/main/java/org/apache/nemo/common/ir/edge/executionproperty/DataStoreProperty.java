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
package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * DataStore ExecutionProperty.
 */
public final class DataStoreProperty extends EdgeExecutionProperty<DataStoreProperty.Value> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private DataStoreProperty(final Value value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static DataStoreProperty of(final Value value) {
    return new DataStoreProperty(value);
  }

  /**
   * Possible values of DataStore ExecutionProperty.
   */
  public enum Value {
    PIPE,
    MEMORY_STORE,
    SERIALIZED_MEMORY_STORE,
    LOCAL_FILE_STORE,
    GLUSTER_FILE_STORE
  }
}
