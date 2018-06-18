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
package edu.snu.nemo.compiler.optimizer.policy;

import edu.snu.nemo.common.exception.CompileTimeOptimizationException;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataFlowModelProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.composite.CompositePass;
import edu.snu.nemo.runtime.common.optimizer.pass.runtime.RuntimePass;

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
  private final Set<Class<? extends ExecutionProperty>> finalizedExecutionProperties;
  private final Set<Class<? extends ExecutionProperty>> annotatedExecutionProperties;
  private final Boolean strictPrerequisiteCheckMode;

  /**
   * Default constructor.
   */
  public PolicyBuilder() {
    this(false);
  }

  /**
   * Constructor.
   *
   * @param strictPrerequisiteCheckMode whether to use strict prerequisite check mode or not.
   */
  public PolicyBuilder(final Boolean strictPrerequisiteCheckMode) {
    this.compileTimePasses = new ArrayList<>();
    this.runtimePasses = new ArrayList<>();
    this.finalizedExecutionProperties = new HashSet<>();
    this.annotatedExecutionProperties = new HashSet<>();
    this.strictPrerequisiteCheckMode = strictPrerequisiteCheckMode;
    // DataCommunicationPattern is already set when creating the IREdge itself.
    annotatedExecutionProperties.add(DataCommunicationPatternProperty.class);
    // Some default values are already annotated.
    annotatedExecutionProperties.add(ExecutorPlacementProperty.class);
    annotatedExecutionProperties.add(ParallelismProperty.class);
    annotatedExecutionProperties.add(DataFlowModelProperty.class);
    annotatedExecutionProperties.add(DataStoreProperty.class);
    annotatedExecutionProperties.add(PartitionerProperty.class);
  }

  /**
   * Register compile time pass.
   * @param compileTimePass the compile time pass to register.
   * @return the PolicyBuilder which registers compileTimePass.
   */
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
      if (strictPrerequisiteCheckMode
          && finalizedExecutionProperties.contains(annotatingPass.getExecutionPropertyToModify())) {
        throw new CompileTimeOptimizationException(annotatingPass.getExecutionPropertyToModify()
            + " should have already been finalized.");
      }
    }
    finalizedExecutionProperties.addAll(compileTimePass.getPrerequisiteExecutionProperties());

    this.compileTimePasses.add(compileTimePass);

    return this;
  }

  /**
   * Register run time passes.
   * @param runtimePass the runtime pass to register.
   * @param runtimePassRegistrator the compile time pass that triggers the runtime pass.
   * @return the PolicyBuilder which registers runtimePass and runtimePassRegistrator.
   */
  public PolicyBuilder registerRuntimePass(final RuntimePass<?> runtimePass,
                                           final CompileTimePass runtimePassRegistrator) {
    registerCompileTimePass(runtimePassRegistrator);
    this.runtimePasses.add(runtimePass);
    return this;
  }

  /**
   * Build a policy using compileTimePasses and runtimePasses in this object.
   * @return the built Policy.
   */
  public Policy build() {
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
