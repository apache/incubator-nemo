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
package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.composite.CompositePass;
import org.apache.nemo.runtime.common.optimizer.pass.runtime.RuntimePass;

import java.util.*;
import java.util.function.Predicate;

/**
 * A builder for policies.
 */
public final class PolicyBuilder {
  private final List<CompileTimePass> compileTimePasses;
  private final List<RuntimePass<?>> runtimePasses;
  private final Set<Class<? extends ExecutionProperty>> annotatedExecutionProperties;

  /**
   * Default constructor.
   */
  public PolicyBuilder() {
    this.compileTimePasses = new ArrayList<>();
    this.runtimePasses = new ArrayList<>();
    this.annotatedExecutionProperties = new HashSet<>();
    // DataCommunicationPattern is already set when creating the IREdge itself.
    annotatedExecutionProperties.add(CommunicationPatternProperty.class);
    // Some default values are already annotated.
    annotatedExecutionProperties.add(ResourcePriorityProperty.class);
    annotatedExecutionProperties.add(ParallelismProperty.class);
    annotatedExecutionProperties.add(DataFlowProperty.class);
    annotatedExecutionProperties.add(DataStoreProperty.class);
    annotatedExecutionProperties.add(PartitionerProperty.class);
  }

  /**
   * Register a compile time pass.
   * @param compileTimePass the compile time pass to register.
   * @return the PolicyBuilder which registers the compileTimePass.
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
      this.annotatedExecutionProperties.addAll(annotatingPass.getExecutionPropertiesToAnnotate());
    }

    this.compileTimePasses.add(compileTimePass);

    return this;
  }

  /**
   * Register compile time pass with its condition under which to run the pass.
   * @param compileTimePass the compile time pass to register.
   * @param condition condition under which to run the pass.
   * @return the PolicyBuilder which registers the compileTimePass.
   */
  public PolicyBuilder registerCompileTimePass(final CompileTimePass compileTimePass,
                                               final Predicate<DAG<IRVertex, IREdge>> condition) {
    compileTimePass.addCondition(condition);
    return this.registerCompileTimePass(compileTimePass);
  }

  /**
   * Register a run time pass.
   * @param runtimePass the runtime pass to register.
   * @param runtimePassRegisterer the compile time pass that triggers the runtime pass.
   * @return the PolicyBuilder which registers the runtimePass and the runtimePassRegisterer.
   */
  public PolicyBuilder registerRuntimePass(final RuntimePass<?> runtimePass,
                                           final CompileTimePass runtimePassRegisterer) {
    registerCompileTimePass(runtimePassRegisterer);
    this.runtimePasses.add(runtimePass);
    return this;
  }

  /**
   * Register a run time pass.
   * @param runtimePass the runtime pass to register.
   * @param runtimePassRegisterer the compile time pass that triggers the runtime pass.
   * @param condition condition under which to run the pass.
   * @return the PolicyBuilder which registers the runtimePass and the runtimePassRegisterer.
   */
  public PolicyBuilder registerRuntimePass(final RuntimePass<?> runtimePass,
                                           final CompileTimePass runtimePassRegisterer,
                                           final Predicate<DAG<IRVertex, IREdge>> condition) {
    runtimePass.addCondition(condition);
    return this.registerRuntimePass(runtimePass, runtimePassRegisterer);
  }

  /**
   * Getter for compile time passes.
   * @return the list of compile time passes.
   */
  public List<CompileTimePass> getCompileTimePasses() {
    return compileTimePasses;
  }

  /**
   * Getter for run time passes.
   * @return the list of run time passes.
   */
  public List<RuntimePass<?>> getRuntimePasses() {
    return runtimePasses;
  }

  /**
   * Build a policy using compileTimePasses and runtimePasses in this object.
   * @return the built Policy.
   */
  public Policy build() {
    return new PolicyImpl(compileTimePasses, runtimePasses);
  }
}
