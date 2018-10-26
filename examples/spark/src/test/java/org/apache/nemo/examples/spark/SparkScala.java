package org.apache.nemo.examples.spark;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.test.ArgBuilder;
import org.apache.nemo.common.test.ExampleTestUtil;
import org.apache.nemo.compiler.optimizer.policy.DefaultPolicy;
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
public final class SparkScala {
  private static final int TIMEOUT = 120000;
  private static ArgBuilder builder;
  private static final String fileBasePath = System.getProperty("user.dir") + "/../resources/";
  private static final String executorResourceFileName = fileBasePath + "spark_test_executor_resources.json";

  @Before
  public void setUp() {
    builder = new ArgBuilder()
        .addResourceJson(executorResourceFileName);
  }

  @Test(timeout = TIMEOUT)
  public void testPi() throws Exception {
    final String numParallelism = "3";

    JobLauncher.main(builder
        .addJobId(SparkPi.class.getSimpleName() + "_test")
        .addUserMain(SparkPi.class.getCanonicalName())
        .addUserArgs(numParallelism)
        .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
        .build());
  }

  @Test(timeout = TIMEOUT)
  public void testWordCount() throws Exception {
    final String inputFileName = "test_input_wordcount_spark";
    final String outputFileName = "test_output_wordcount_spark";
    final String expectedOutputFilename = "expected_output_wordcount_spark";
    final String inputFilePath = fileBasePath + inputFileName;
    final String outputFilePath = fileBasePath + outputFileName;

    JobLauncher.main(builder
        .addJobId(SparkWordCount.class.getSimpleName() + "_test")
        .addUserMain(SparkWordCount.class.getCanonicalName())
        .addUserArgs(inputFilePath, outputFilePath)
        .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
        .build());

    try {
      ExampleTestUtil.ensureOutputValidity(fileBasePath, outputFileName, expectedOutputFilename);
    } finally {
      ExampleTestUtil.deleteOutputFile(fileBasePath, outputFileName);
    }
  }

  @Test(timeout = TIMEOUT)
  public void testCachingWordCount() throws Exception {
    final String inputFileName = "test_input_wordcount_spark";
    final String outputFileName1 = "test_output_wordcount_spark";
    final String outputFileName2 = "test_output_reversed_wordcount_spark";
    final String expectedOutputFilename1 = "expected_output_wordcount_spark";
    final String expectedOutputFilename2 = "expected_output_reversed_wordcount_spark";
    final String inputFilePath = fileBasePath + inputFileName;
    final String outputFilePath1 = fileBasePath + outputFileName1;
    final String outputFilePath2 = fileBasePath + outputFileName2;

    JobLauncher.main(builder
        .addJobId(SparkCachingWordCount.class.getSimpleName() + "_test")
        .addUserMain(SparkCachingWordCount.class.getCanonicalName())
        .addUserArgs(inputFilePath, outputFilePath1, outputFilePath2)
        .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
        .build());

    try {
      ExampleTestUtil.ensureOutputValidity(fileBasePath, outputFileName1, expectedOutputFilename1);
      ExampleTestUtil.ensureOutputValidity(fileBasePath, outputFileName2, expectedOutputFilename2);
    } finally {
      ExampleTestUtil.deleteOutputFile(fileBasePath, outputFileName1);
      ExampleTestUtil.deleteOutputFile(fileBasePath, outputFileName2);
    }
  }

  @Test(timeout = TIMEOUT)
  public void testALS() throws Exception {
    JobLauncher.main(builder
      .addJobId(SparkALS.class.getSimpleName() + "_test")
      .addUserMain(SparkALS.class.getCanonicalName())
      .addUserArgs("100") // TODO #202: Bug with empty string user_args
      .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
      .build());
  }
}
