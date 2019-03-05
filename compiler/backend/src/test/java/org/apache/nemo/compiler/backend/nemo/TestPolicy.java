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
package org.apache.nemo.compiler.backend.nemo;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultScheduleGroupPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.ShuffleEdgePushPass;
import org.apache.nemo.compiler.optimizer.pass.runtime.Message;
import org.apache.nemo.compiler.optimizer.policy.Policy;
import org.apache.nemo.compiler.optimizer.policy.PolicyImpl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * A policy for tests.
 */
public final class TestPolicy implements Policy {
  private final Policy policy;

  public TestPolicy() {
    this(false);
  }

  public TestPolicy(final boolean testPushPolicy) {
    List<CompileTimePass> compileTimePasses = new ArrayList<>();

    if (testPushPolicy) {
      compileTimePasses.add(new ShuffleEdgePushPass());
    }

    compileTimePasses.add(new DefaultScheduleGroupPass());

    this.policy = new PolicyImpl(compileTimePasses, new HashSet<>());
  }

  @Override
  public IRDAG runCompileTimeOptimization(final IRDAG dag, final String dagDirectory) {
    return this.policy.runCompileTimeOptimization(dag, dagDirectory);
  }

  @Override
  public IRDAG runRunTimeOptimizations(final IRDAG dag, final Message<?> message) {
    return this.policy.runRunTimeOptimizations(dag, message);
  }
}
