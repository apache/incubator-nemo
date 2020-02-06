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
package org.apache.nemo.examples.beam.policy;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.compiler.optimizer.pass.runtime.Message;
import org.apache.nemo.compiler.optimizer.policy.Policy;
import org.apache.nemo.compiler.optimizer.policy.PolicyImpl;
import org.apache.nemo.compiler.optimizer.policy.TransientResourcePolicy;

/**
 * A transient resource policy with fixed parallelism 10 for tests.
 */
public final class TransientResourcePolicyParallelismTen implements Policy {
  private final Policy policy;

  public TransientResourcePolicyParallelismTen() {
    this.policy = new PolicyImpl(
      PolicyTestUtil.overwriteParallelism(10,
        TransientResourcePolicy.BUILDER.getCompileTimePasses()),
      TransientResourcePolicy.BUILDER.getRunTimePasses());
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
