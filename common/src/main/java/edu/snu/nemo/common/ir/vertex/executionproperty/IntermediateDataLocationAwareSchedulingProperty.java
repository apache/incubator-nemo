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
package edu.snu.nemo.common.ir.vertex.executionproperty;

import edu.snu.nemo.common.ir.executionproperty.VertexExecutionProperty;

/**
 * This property decides whether or not to schedule this vertex only on executors where intermediate data reside.
 */
public final class IntermediateDataLocationAwareSchedulingProperty extends VertexExecutionProperty<Boolean> {
  private static final IntermediateDataLocationAwareSchedulingProperty INTERM_TRUE
      = new IntermediateDataLocationAwareSchedulingProperty(true);
  private static final IntermediateDataLocationAwareSchedulingProperty INTERM_FALSE
      = new IntermediateDataLocationAwareSchedulingProperty(false);

  /**
   * Default constructor.
   *
   * @param value value of the ExecutionProperty
   */
  private IntermediateDataLocationAwareSchedulingProperty(final boolean value) {
    super(value);
  }

  /**
   * Static method getting execution property.
   *
   * @param value value of the new execution property
   * @return the execution property
   */
  public static IntermediateDataLocationAwareSchedulingProperty of(final boolean value) {
    return value ? INTERM_TRUE : INTERM_FALSE;
  }
}
