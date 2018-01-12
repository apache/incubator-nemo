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
package edu.snu.onyx.examples.spark;

import edu.snu.onyx.client.JobLauncher;
import edu.snu.onyx.common.ArgBuilder;
import edu.snu.onyx.compiler.optimizer.policy.DefaultPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test SparkPi program with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class SparkPiITCase {
  private static final int TIMEOUT = 60000;
  private static final String numParallelism = "10";

  private static ArgBuilder builder = new ArgBuilder()
      .addJobId(SparkPiITCase.class.getSimpleName())
      .addUserMain(JavaSparkPi.class.getCanonicalName())
      .addUserArgs(numParallelism);

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
        .addUserMain(JavaSparkPi.class.getCanonicalName())
        .addUserArgs(numParallelism);
  }

  @Test(timeout = TIMEOUT)
  public void test() throws Exception {
    JobLauncher.main(builder
        .addJobId(SparkPiITCase.class.getSimpleName())
        .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
        .build());
  }
}
