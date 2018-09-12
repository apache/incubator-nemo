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
import org.apache.nemo.examples.beam.tpch.Tpch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test TPC-H program with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class SQLTpchITCase {
  private static final int TIMEOUT = 180000;
  private static ArgBuilder builder;
  private static final String fileBasePath = System.getProperty("user.dir") + "/../resources/";

  private static final String outputFileName = "test_output_tpch";
  private static final String expectedOutputFileName = "expected_output_tpch";
  private static final String executorResourceFileName = fileBasePath + "beam_test_executor_resources.json";
  private static final String outputFilePath =  fileBasePath + outputFileName;

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
        .addResourceJson(executorResourceFileName);
  }

  @After
  public void tearDown() throws Exception {
    try {
      ExampleTestUtil.ensureOutputValidity(fileBasePath, outputFileName, expectedOutputFileName);
    } finally {
      ExampleTestUtil.deleteOutputFile(fileBasePath, outputFileName);
    }
  }

  @Test (timeout = TIMEOUT)
  public void testXX() throws Exception {
    final int queryNum = 12;
    JobLauncher.main(builder
      .addUserMain(Tpch.class.getCanonicalName())
      .addUserArgs(String.valueOf(queryNum), "/home/johnyangk/Desktop/tpc-concat-tbls/", outputFilePath)
      .addJobId(SQLTpchITCase.class.getSimpleName())
      .addOptimizationPolicy(DefaultPolicyParallelismFive.class.getCanonicalName())
      .build());
  }

  @Test (timeout = TIMEOUT)
  public void testThree() throws Exception {
    final int queryNum = 3;
    JobLauncher.main(builder
      .addUserMain(Tpch.class.getCanonicalName())
      .addUserArgs(String.valueOf(queryNum), "/home/johnyangk/Desktop/tpc-concat-tbls/", outputFilePath)
      .addJobId(SQLTpchITCase.class.getSimpleName())
      .addOptimizationPolicy(DefaultPolicyParallelismFive.class.getCanonicalName())
      .build());
  }

  @Test (timeout = TIMEOUT)
  public void testFour() throws Exception {
    final int queryNum = 4;
    JobLauncher.main(builder
      .addUserMain(Tpch.class.getCanonicalName())
      .addUserArgs(String.valueOf(queryNum), "/home/johnyangk/Desktop/tpc-concat-tbls/", outputFilePath)
      .addJobId(SQLTpchITCase.class.getSimpleName())
      .addOptimizationPolicy(DefaultPolicyParallelismFive.class.getCanonicalName())
      .build());
  }

  @Test (timeout = TIMEOUT)
  public void testFive() throws Exception {
    final int queryNum = 5;
    JobLauncher.main(builder
      .addUserMain(Tpch.class.getCanonicalName())
      .addUserArgs(String.valueOf(queryNum), "/home/johnyangk/Desktop/tpc-concat-tbls/", outputFilePath)
      .addJobId(SQLTpchITCase.class.getSimpleName())
      .addOptimizationPolicy(DefaultPolicyParallelismFive.class.getCanonicalName())
      .build());
  }

  @Test (timeout = TIMEOUT)
  public void testSix() throws Exception {
    final int queryNum = 6;
    JobLauncher.main(builder
      .addUserMain(Tpch.class.getCanonicalName())
      .addUserArgs(String.valueOf(queryNum), "/home/johnyangk/Desktop/tpc-concat-tbls/", outputFilePath)
      .addJobId(SQLTpchITCase.class.getSimpleName())
      .addOptimizationPolicy(DefaultPolicyParallelismFive.class.getCanonicalName())
      .build());
  }
}
