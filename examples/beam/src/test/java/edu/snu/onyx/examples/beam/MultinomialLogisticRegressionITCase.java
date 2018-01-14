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
package edu.snu.onyx.examples.beam;

import edu.snu.onyx.client.JobLauncher;
import edu.snu.onyx.common.ArgBuilder;
import edu.snu.onyx.compiler.optimizer.policy.PadoPolicy;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Testing Multinomial Logistic Regressions with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class MultinomialLogisticRegressionITCase {
  private static final int TIMEOUT = 120000;
  private static final String input = System.getProperty("user.dir") + "/../resources/sample_input_mlr";
  private static final String numFeatures = "100";
  private static final String numClasses = "5";
  private static final String numIteration = "3";

  private static ArgBuilder builder = new ArgBuilder()
      .addJobId(MultinomialLogisticRegressionITCase.class.getSimpleName())
      .addUserMain(MultinomialLogisticRegression.class.getCanonicalName())
      .addUserArgs(input, numFeatures, numClasses, numIteration);

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
        .addUserMain(MultinomialLogisticRegression.class.getCanonicalName())
        .addUserArgs(input, numFeatures, numClasses, numIteration);
  }

//  @Test (timeout = TIMEOUT)
//  public void test() throws Exception {
//    JobLauncher.main(builder
//        .addJobId(MultinomialLogisticRegressionITCase.class.getSimpleName())
//        .build());
//  }

  @Test (timeout = TIMEOUT)
  public void testPado() throws Exception {
    JobLauncher.main(builder
        .addJobId(MultinomialLogisticRegressionITCase.class.getSimpleName() + "_pado")
        .addOptimizationPolicy(PadoPolicy.class.getCanonicalName())
        .build());
  }
}
