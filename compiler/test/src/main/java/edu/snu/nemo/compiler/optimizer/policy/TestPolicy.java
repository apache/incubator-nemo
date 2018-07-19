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

import edu.snu.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.*;
import edu.snu.nemo.runtime.common.optimizer.pass.runtime.RuntimePass;

import java.util.*;

/**
 * A policy for tests.
 */
public final class TestPolicy implements Policy {
  private final boolean testPushPolicy;

  public TestPolicy() {
    this(false);
  }

  public TestPolicy(final boolean testPushPolicy) {
    this.testPushPolicy = testPushPolicy;
  }

  @Override
  public List<CompileTimePass> getCompileTimePasses() {
    List<CompileTimePass> policy = new ArrayList<>();

    if (testPushPolicy) {
      policy.add(new ShuffleEdgePushPass());
    }

    policy.add(new DefaultScheduleGroupPass());
    return policy;
  }

  @Override
  public List<RuntimePass<?>> getRuntimePasses() {
    return new ArrayList<>();
  }
}
