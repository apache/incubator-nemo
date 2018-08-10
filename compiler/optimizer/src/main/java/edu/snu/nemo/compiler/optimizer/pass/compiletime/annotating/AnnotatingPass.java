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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A compile-time pass that annotates the IR DAG with execution properties.
 * It is ensured by the compiler that the shape of the IR DAG itself is not modified by an AnnotatingPass.
 */
public abstract class AnnotatingPass extends CompileTimePass {
  private final Set<Class<? extends ExecutionProperty>> keyOfExecutionPropertyToModify;
  private final Set<Class<? extends ExecutionProperty>> prerequisiteExecutionProperties;

  /**
   * Constructor.
   * @param cls the annotating pass class.
   */
  public AnnotatingPass(final Class<? extends AnnotatingPass> cls) {
    final Annotates annotates = cls.getAnnotation(Annotates.class);
    this.keyOfExecutionPropertyToModify = new HashSet<>(Arrays.asList(annotates.value()));

    final Requires requires = cls.getAnnotation(Requires.class);
    this.prerequisiteExecutionProperties = new HashSet<>(Arrays.asList(requires.value()));
  }

  /**
   * Getter for key of execution property to modify.
   * @return key of execution property to modify.
   */
  public final Set<Class<? extends ExecutionProperty>> getExecutionPropertyToModify() {
    return keyOfExecutionPropertyToModify;
  }

  @Override
  public final Set<Class<? extends ExecutionProperty>> getPrerequisiteExecutionProperties() {
    return prerequisiteExecutionProperties;
  }
}
