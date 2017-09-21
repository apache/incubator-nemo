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
package edu.snu.vortex.examples.beam;

import edu.snu.vortex.client.JobLauncher;
import edu.snu.vortex.compiler.CompilerTestUtil;
import edu.snu.vortex.compiler.optimizer.policy.PadoPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test Alternating Least Square program with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class AlternatingLeastSquareITCase {
  private static final int TIMEOUT = 120000;
  private static final String input = CompilerTestUtil.rootDir + "/src/main/resources/sample_input_als";
  private static final String numFeatures = "10";
  private static final String numIteration = "3";
  private static final String dagDirectory = "./dag";

  public static ArgBuilder builder = new ArgBuilder()
      .addJobId(AlternatingLeastSquareITCase.class.getSimpleName())
      .addUserMain(AlternatingLeastSquare.class.getCanonicalName())
      .addUserArgs(input, numFeatures, numIteration)
      .addDAGDirectory(dagDirectory);

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
        .addUserMain(AlternatingLeastSquare.class.getCanonicalName())
        .addUserArgs(input, numFeatures, numIteration)
        .addDAGDirectory(dagDirectory);
  }

  // Disabled for speed purposes. To be re-enabled through TODO #138.
//  @Test (timeout = TIMEOUT)
//  public void test() throws Exception {
//    JobLauncher.main(builder
//        .addJobId(AlternatingLeastSquareITCase.class.getSimpleName())
//        .build());
//  }

  @Test (timeout = TIMEOUT)
  public void testPado() throws Exception {
    JobLauncher.main(builder
        .addJobId(AlternatingLeastSquareITCase.class.getSimpleName() + "_pado")
        .addOptimizationPolicy(PadoPolicy.class.getCanonicalName())
        .build());
  }
}
