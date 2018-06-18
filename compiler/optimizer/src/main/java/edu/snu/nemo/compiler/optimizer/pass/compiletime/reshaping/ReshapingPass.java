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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.reshaping;

import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;

import java.util.HashSet;
import java.util.Set;

/**
 * A compile-time pass that reshapes the structure of the IR DAG.
 * It is ensured by the compiler that no execution properties are modified by a ReshapingPass.
 */
public abstract class ReshapingPass implements CompileTimePass {
  private final Set<Class<? extends ExecutionProperty>> prerequisiteExecutionProperties;

  /**
   * Default constructor.
   */
  public ReshapingPass() {
    this.prerequisiteExecutionProperties = new HashSet<>();
  }

  /**
   * Constructor.
   * @param prerequisiteExecutionProperties prerequisite of execution properties.
   */
  public ReshapingPass(final Set<Class<? extends ExecutionProperty>> prerequisiteExecutionProperties) {
    this.prerequisiteExecutionProperties = prerequisiteExecutionProperties;
  }

  @Override
  public final Set<Class<? extends ExecutionProperty>> getPrerequisiteExecutionProperties() {
    return prerequisiteExecutionProperties;
  }
}
