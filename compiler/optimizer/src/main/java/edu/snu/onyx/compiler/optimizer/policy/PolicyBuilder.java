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
package edu.snu.onyx.compiler.optimizer.policy;

import edu.snu.onyx.common.exception.CompileTimeOptimizationException;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.CompileTimePass;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.composite.CompositePass;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.composite.InitiationCompositePass;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.reshaping.ReshapingPass;
import edu.snu.onyx.runtime.common.optimizer.pass.runtime.RuntimePass;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A builder for policies.
 */
public final class PolicyBuilder {
  private final List<CompileTimePass> compileTimePasses;
  private final List<RuntimePass<?>> runtimePasses;
  private final Set<ExecutionProperty.Key> requiredExecutionProperties;
  private final Set<ExecutionProperty.Key> annotatedExecutionProperties;

  public PolicyBuilder() {
    this.compileTimePasses = new ArrayList<>();
    this.runtimePasses = new ArrayList<>();
    this.requiredExecutionProperties = new HashSet<>();
    this.annotatedExecutionProperties = new HashSet<>();
    // DataCommunicationPattern is already set when creating the IREdge itself.
    annotatedExecutionProperties.add(ExecutionProperty.Key.DataCommunicationPattern);
  }

  public PolicyBuilder registerCompileTimePass(final CompileTimePass compileTimePass) {
    // We decompose CompositePasses.
    if (compileTimePass instanceof CompositePass) {
      final CompositePass compositePass = (CompositePass) compileTimePass;
      compositePass.getPassList().forEach(this::registerCompileTimePass);
      return this;
    }

    // Check prerequisite execution properties.
    if (!annotatedExecutionProperties.containsAll(compileTimePass.getPrerequisiteExecutionProperties())) {
      throw new CompileTimeOptimizationException("Prerequisite ExecutionProperty hasn't been met for "
          + compileTimePass.getClass().getSimpleName());
    }

    // check annotation of annotating passes.
    if (compileTimePass instanceof AnnotatingPass) {
      final AnnotatingPass annotatingPass = (AnnotatingPass) compileTimePass;
      this.annotatedExecutionProperties.add(annotatingPass.getExecutionPropertyToModify());
    }
    this.compileTimePasses.add(compileTimePass);

    // re-initiate after each reshaping pass.
    if (compileTimePass instanceof ReshapingPass) {
      this.registerCompileTimePass(new InitiationCompositePass());
    }
    return this;
  }

  public PolicyBuilder registerRuntimePass(final RuntimePass<?> runtimePass,
                                           final CompileTimePass runtimePassRegistrator) {
    registerCompileTimePass(runtimePassRegistrator);
    this.runtimePasses.add(runtimePass);
    return this;
  }

  public PolicyBuilder addExecutionPropertyRequirement(final ExecutionProperty.Key key) {
    requiredExecutionProperties.add(key);
    return this;
  }

  public Policy build() {
    // see if required execution properties have been met
    if (!annotatedExecutionProperties.containsAll(requiredExecutionProperties)) {
      throw new CompileTimeOptimizationException("Required execution properties has not been met for the policy");
    }

    return new Policy() {
      @Override
      public List<CompileTimePass> getCompileTimePasses() {
        return compileTimePasses;
      }

      @Override
      public List<RuntimePass<?>> getRuntimePasses() {
        return runtimePasses;
      }
    };
  }
}
