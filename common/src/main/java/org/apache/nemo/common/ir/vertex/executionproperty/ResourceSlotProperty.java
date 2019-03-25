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
package org.apache.nemo.common.ir.vertex.executionproperty;

import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

/**
 * This property decides whether or not to comply to slot restrictions when scheduling this vertex.
 */
public final class ResourceSlotProperty extends VertexExecutionProperty<Boolean> {
  private static final ResourceSlotProperty COMPLIANCE_TRUE = new ResourceSlotProperty(true);
  private static final ResourceSlotProperty COMPLIANCE_FALSE
    = new ResourceSlotProperty(false);

  /**
   * Default constructor.
   *
   * @param value value of the ExecutionProperty
   */
  private ResourceSlotProperty(final boolean value) {
    super(value);
  }

  /**
   * Static method getting execution property.
   *
   * @param value value of the new execution property
   * @return the execution property
   */
  public static ResourceSlotProperty of(final boolean value) {
    return value ? COMPLIANCE_TRUE : COMPLIANCE_FALSE;
  }
}
