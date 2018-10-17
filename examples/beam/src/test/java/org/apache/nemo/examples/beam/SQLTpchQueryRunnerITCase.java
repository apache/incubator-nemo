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
package org.apache.nemo.examples.beam;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.test.ArgBuilder;
import org.apache.nemo.examples.beam.policy.DefaultPolicyParallelismFive;
import org.apache.nemo.examples.beam.tpch.TpchQueryRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import java.util.Arrays;
import java.util.Collection;

/**
 * Test TPC-H program with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Parameterized.class)
@PrepareForTest(JobLauncher.class)
public final class SQLTpchQueryRunnerITCase {
  private static final int TIMEOUT = 180000;
  private static ArgBuilder builder;
  private static final String fileBasePath = System.getProperty("user.dir") + "/../resources/";
  private static final String executorResourceFileName = fileBasePath + "beam_test_executor_resources.json";
  private static final String tableDirectoryPath = fileBasePath + "tpch/tables/";

  @Parameterized.Parameters
  public static Collection<Integer> queries() {
    return Arrays.asList(3);
    /*
    return Arrays.asList(3, 4, 5, 6, 10, 12, 13, 14);
    */
  }

  private final String queryFilePath;
  private final String outputFilePath;


  public SQLTpchQueryRunnerITCase(final int queryNum) {
    this.queryFilePath = String.format("%stpch/queries/tpch_query%d.sql", fileBasePath, queryNum);
    this.outputFilePath = String.format("%stpch/test_output_%d", fileBasePath, queryNum);
  }

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
        .addResourceJson(executorResourceFileName);
  }

  @After
  public void tearDown() throws Exception {
    /*
    try {
      ExampleTestUtil.ensureOutputValidity(fileBasePath, outputFileName, expectedOutputFileName);
    } finally {
      ExampleTestUtil.deleteOutputFile(fileBasePath, outputFileName);
    }
    */
  }

  @Test (timeout = TIMEOUT)
  public void test() throws Exception {
    JobLauncher.main(builder
      .addUserMain(TpchQueryRunner.class.getCanonicalName())
      .addUserArgs(queryFilePath, tableDirectoryPath, outputFilePath)
      .addJobId(SQLTpchQueryRunnerITCase.class.getSimpleName())
      .addOptimizationPolicy(DefaultPolicyParallelismFive.class.getCanonicalName())
      .build());
  }
}
