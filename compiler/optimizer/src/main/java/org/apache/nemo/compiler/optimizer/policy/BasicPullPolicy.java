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
package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.AggressiveSpeculativeCloningPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultScheduleGroupPass;
import org.apache.reef.tang.Injector;

/**
 * Basic pull policy.
 * TODO #200: Maintain Test Passes and Policies Separately
 */
public final class BasicPullPolicy implements Policy {
  public static final PolicyBuilder BUILDER =
      new PolicyBuilder()
          .registerCompileTimePass(new AggressiveSpeculativeCloningPass())
          .registerCompileTimePass(new DefaultScheduleGroupPass());
  private final Policy policy;

  /**
   * Default constructor.
   */
  public BasicPullPolicy() {
    this.policy = BUILDER.build();
  }

  @Override
  public IRDAG runCompileTimeOptimization(final IRDAG dag, final String dagDirectory) {
    return this.policy.runCompileTimeOptimization(dag, dagDirectory);
  }

  @Override
  public void registerRunTimeOptimizations(final Injector injector, final PubSubEventHandlerWrapper pubSubWrapper) {
    this.policy.registerRunTimeOptimizations(injector, pubSubWrapper);
  }
}
