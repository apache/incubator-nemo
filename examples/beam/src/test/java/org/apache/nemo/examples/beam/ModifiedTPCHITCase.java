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
import org.apache.nemo.common.test.ExampleTestUtil;
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
 * Test modified TPC-H queries.
 * (1) Queries: 'WHERE' clauses with constant values have been removed to ensure the final query result is non-null.
 * (2) Tables: Some rows are modified such that the results of joins are non-null.
 */
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Parameterized.class)
@PrepareForTest(JobLauncher.class)
public final class ModifiedTPCHITCase {
  private static final int TIMEOUT = 180000;
  private static ArgBuilder builder;
  private static final String fileBasePath = System.getProperty("user.dir") + "/../resources/";
  private static final String tpchTestPath = fileBasePath + "tpch/";
  private static final String executorResourceFileName = fileBasePath + "beam_test_executor_resources.json";
  private static final String tableDirectoryPath = fileBasePath + "tpch/tables/";

  @Parameterized.Parameters
  public static Collection<Integer> queries() {
    return Arrays.asList(3, 4, 6, 10, 12, 13, 14);
  }

  private final int queryNum;

  public ModifiedTPCHITCase(final int queryNum) {
    this.queryNum = queryNum;
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
      ExampleTestUtil.ensureSQLOutputValidity(tpchTestPath, getOutputFileName(), getExpectedOutputFileName());
    } finally {
      ExampleTestUtil.deleteOutputFile(tpchTestPath, getOutputFileName());
    }
    */
  }

  @Test (timeout = TIMEOUT)
  public void test() throws Exception {
    JobLauncher.main(builder
      .addUserMain(TpchQueryRunner.class.getCanonicalName())
      .addUserArgs(getQueryFilePath(), tableDirectoryPath, tpchTestPath + getOutputFileName())
      .addJobId(ModifiedTPCHITCase.class.getSimpleName())
      .addOptimizationPolicy(DefaultPolicyParallelismFive.class.getCanonicalName())
      .build());
  }

  private String getQueryFilePath() {
    return String.format("%smodified_queries/tpch_query%d.sql", tpchTestPath, queryNum);
  }

  private String getOutputFileName() {
    return String.format("test_output_%d_", queryNum);
  }

  private String getExpectedOutputFileName() {
    return String.format("expected_output_%d", queryNum);
  }
}
