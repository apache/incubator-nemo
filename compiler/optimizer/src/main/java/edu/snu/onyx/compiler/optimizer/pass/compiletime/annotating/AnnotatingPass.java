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
package edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.CompileTimePass;

import java.util.HashSet;
import java.util.Set;

/**
 * A compile-time pass that annotates the IR DAG with execution properties.
 * It is ensured by the compiler that the shape of the IR DAG itself is not modified by an AnnotatingPass.
 */
public abstract class AnnotatingPass implements CompileTimePass {
  private final ExecutionProperty.Key keyOfExecutionPropertyToModify;
  private final Set<ExecutionProperty.Key> prerequisiteExecutionProperties;

  public AnnotatingPass(final ExecutionProperty.Key keyOfExecutionPropertyToModify,
                        final Set<ExecutionProperty.Key> prerequisiteExecutionProperties) {
    this.keyOfExecutionPropertyToModify = keyOfExecutionPropertyToModify;
    this.prerequisiteExecutionProperties = prerequisiteExecutionProperties;
  }

  public AnnotatingPass(final ExecutionProperty.Key keyOfExecutionPropertyToModify) {
    this.keyOfExecutionPropertyToModify = keyOfExecutionPropertyToModify;
    this.prerequisiteExecutionProperties = new HashSet<>();
  }

  public final ExecutionProperty.Key getExecutionPropertyToModify() {
    return keyOfExecutionPropertyToModify;
  }

  @Override
  public final Set<ExecutionProperty.Key> getPrerequisiteExecutionProperties() {
    return prerequisiteExecutionProperties;
  }
}
