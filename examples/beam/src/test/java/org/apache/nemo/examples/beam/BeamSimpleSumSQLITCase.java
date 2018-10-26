package org.apache.nemo.examples.beam;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.test.ArgBuilder;
import org.apache.nemo.common.test.ExampleTestUtil;
import org.apache.nemo.examples.beam.policy.DefaultPolicyParallelismFive;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test Broadcast program with JobLauncher.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class BeamSimpleSumSQLITCase {
  private static final int TIMEOUT = 180000;
  private static ArgBuilder builder;
  private static final String fileBasePath = System.getProperty("user.dir") + "/../resources/";

  private static final String outputFileName = "test_output_simplesql";
  private static final String expectedOutputFileName = "expected_output_simplesql";
  private static final String executorResourceFileName = fileBasePath + "beam_test_executor_resources.json";
  private static final String outputFilePath =  fileBasePath + outputFileName;

  @Before
  public void setUp() throws Exception {
    builder = new ArgBuilder()
        .addUserMain(SimpleSumSQL.class.getCanonicalName())
        .addUserArgs(outputFilePath)
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
  public void test() throws Exception {
    JobLauncher.main(builder
        .addJobId(BeamSimpleSumSQLITCase.class.getSimpleName())
        .addOptimizationPolicy(DefaultPolicyParallelismFive.class.getCanonicalName())
        .build());
  }
}
