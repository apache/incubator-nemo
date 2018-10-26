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
 * Test MR Spark programs with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
@PowerMockIgnore("javax.management.*")
public final class MRJava {
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
  public void testSparkWordCount() throws Exception {
    final String inputFileName = "test_input_wordcount_spark";
    final String outputFileName = "test_output_wordcount_spark";
    final String expectedOutputFilename = "expected_output_wordcount_spark";
    final String inputFilePath = fileBasePath + inputFileName;
    final String outputFilePath = fileBasePath + outputFileName;

    JobLauncher.main(builder
        .addJobId(JavaWordCount.class.getSimpleName() + "_test")
        .addUserMain(JavaWordCount.class.getCanonicalName())
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
  public void testSparkWordAndLineCount() throws Exception {
    final String inputFileName = "test_input_wordcount_spark";
    final String outputFileName = "test_output_word_and_line_count";
    final String expectedOutputFilename = "expected_output_word_and_line_count";
    final String inputFilePath = fileBasePath + inputFileName;
    final String outputFilePath = fileBasePath + outputFileName;

    JobLauncher.main(builder
        .addJobId(JavaWordAndLineCount.class.getSimpleName() + "_test")
        .addUserMain(JavaWordAndLineCount.class.getCanonicalName())
        .addUserArgs(inputFilePath, outputFilePath)
        .addOptimizationPolicy(DefaultPolicy.class.getCanonicalName())
        .build());

    try {
      ExampleTestUtil.ensureOutputValidity(fileBasePath, outputFileName, expectedOutputFilename);
    } finally {
      ExampleTestUtil.deleteOutputFile(fileBasePath, outputFileName);
    }
  }
}
