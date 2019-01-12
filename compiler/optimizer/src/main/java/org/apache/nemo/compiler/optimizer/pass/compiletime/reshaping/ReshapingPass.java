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
package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A compile-time pass that reshapes the structure of the IR DAG.
 * It is ensured by the compiler that no execution properties are modified by a ReshapingPass.
 */
public abstract class ReshapingPass extends CompileTimePass {
  private final Set<Class<? extends ExecutionProperty>> prerequisiteExecutionProperties;

  /**
   * Constructor.
   * @param cls the reshaping pass class.
   */
  public ReshapingPass(final Class<? extends ReshapingPass> cls) {
    final Requires requires = cls.getAnnotation(Requires.class);
    this.prerequisiteExecutionProperties = requires == null
        ? new HashSet<>() : new HashSet<>(Arrays.asList(requires.value()));
  }

  /**
   * @return the prerequisite execution properties.
   */
  public final Set<Class<? extends ExecutionProperty>> getPrerequisiteExecutionProperties() {
    return prerequisiteExecutionProperties;
  }
}
