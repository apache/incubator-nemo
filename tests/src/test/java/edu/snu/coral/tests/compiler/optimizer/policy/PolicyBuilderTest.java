package edu.snu.coral.tests.compiler.optimizer.policy;

import edu.snu.coral.common.exception.CompileTimeOptimizationException;
import edu.snu.coral.compiler.optimizer.pass.compiletime.annotating.DefaultStagePartitioningPass;
import edu.snu.coral.compiler.optimizer.pass.compiletime.annotating.ScheduleGroupPass;
import edu.snu.coral.compiler.optimizer.pass.compiletime.composite.PadoCompositePass;
import edu.snu.coral.compiler.optimizer.policy.*;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class PolicyBuilderTest {
  @Test
  public void testDisaggregationPolicy() {
    final Policy disaggregationPolicy = new DisaggregationPolicy();
    assertEquals(10, disaggregationPolicy.getCompileTimePasses().size());
    assertEquals(0, disaggregationPolicy.getRuntimePasses().size());
  }

  @Test
  public void testPadoPolicy() {
    final Policy padoPolicy = new PadoPolicy();
    assertEquals(12, padoPolicy.getCompileTimePasses().size());
    assertEquals(0, padoPolicy.getRuntimePasses().size());
  }

  @Test
  public void testDataSkewPolicy() {
    final Policy dataSkewPolicy = new DataSkewPolicy();
    assertEquals(14, dataSkewPolicy.getCompileTimePasses().size());
    assertEquals(1, dataSkewPolicy.getRuntimePasses().size());
  }

  @Test
  public void testShouldFailPolicy() {
    try {
      final Policy failPolicy = new PolicyBuilder()
          .registerCompileTimePass(new PadoCompositePass())
          .registerCompileTimePass(new DefaultStagePartitioningPass())
          .registerCompileTimePass(new ScheduleGroupPass())
          .build();
    } catch (Exception e) { // throw an exception if default execution properties are not set.
      assertTrue(e instanceof CompileTimeOptimizationException);
      assertTrue(e.getMessage().contains("Prerequisite ExecutionProperty hasn't been met"));
    }
  }
}
