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
package edu.snu.nemo.examples.spark;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.test.ArgBuilder;
import edu.snu.nemo.common.test.ExampleTestUtil;
import edu.snu.nemo.compiler.optimizer.policy.DefaultPolicy;
import edu.snu.nemo.examples.spark.sql.JavaUserDefinedTypedAggregation;
import edu.snu.nemo.examples.spark.sql.JavaUserDefinedUntypedAggregation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test Spark programs with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
@PowerMockIgnore("javax.management.*")
public final class SparkJava {
  private static final int TIMEOUT = 180000;
  private static ArgBuilder builder;
  private static final String fileBasePath = System.getProperty("user.dir") + "/../resources/";
  private static final String executorResourceFileName = fileBasePath + "spark_test_executor_resources.json";

  @Before
  public void setUp() {
    builder = new ArgBuilder()
        .addResourceJson(executorResourceFileName);
  }

  @Test(timeout = TIMEOUT)
  public void testSparkPi() throws Exception {
    final String numParallelism = "3";

    JobLauncher.main(builder
        .addJobId(JavaSparkPi.class.getSimpleName() + "_test")
        .addUserMain(JavaSparkPi.class.getCanonicalName())
        .addUserArgs(numParallelism)
        .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
        .build());
  }

  @Test(timeout = TIMEOUT)
  public void testSparkSQLUserDefinedTypedAggregation() throws Exception {
    final String inputFileName = "test_input_employees.json";
    final String inputFilePath = fileBasePath + inputFileName;

    JobLauncher.main(builder
        .addJobId(JavaUserDefinedTypedAggregation.class.getSimpleName() + "_test")
        .addUserMain(JavaUserDefinedTypedAggregation.class.getCanonicalName())
        .addUserArgs(inputFilePath)
        .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
        .build());
  }

  @Test(timeout = TIMEOUT)
  public void testSparkSQLUserDefinedUntypedAggregation() throws Exception {
    final String inputFileName = "test_input_employees.json";
    final String inputFilePath = fileBasePath + inputFileName;

    JobLauncher.main(builder
        .addJobId(JavaUserDefinedUntypedAggregation.class.getSimpleName() + "_test")
        .addUserMain(JavaUserDefinedUntypedAggregation.class.getCanonicalName())
        .addUserArgs(inputFilePath)
        .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
        .build());
  }

  @Test(timeout = TIMEOUT)
  public void testSparkSQLExample() throws Exception {
    final String peopleJson = "test_input_people.json";
    final String peopleTxt = "test_input_people.txt";
    final String inputFileJson = fileBasePath + peopleJson;
    final String inputFileTxt = fileBasePath + peopleTxt;

    //    TODO#12: Frontend support for Scala Spark.
    //    JobLauncher.main(builder
    //        .addJobId(JavaSparkSQLExample.class.getSimpleName() + "_test")
    //        .addUserMain(JavaSparkSQLExample.class.getCanonicalName())
    //        .addUserArgs(inputFileJson, inputFileTxt)
    //        .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
    //        .build());
  }
}
