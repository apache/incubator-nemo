package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultScheduleGroupPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.composite.TransientResourceCompositePass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class PolicyBuilderTest {
  @Test
  public void testDisaggregationPolicy() {
    assertEquals(18, DisaggregationPolicy.BUILDER.getCompileTimePasses().size());
    assertEquals(0, DisaggregationPolicy.BUILDER.getRuntimePasses().size());
  }

  @Test
  public void testTransientResourcePolicy() {
    assertEquals(20, TransientResourcePolicy.BUILDER.getCompileTimePasses().size());
    assertEquals(0, TransientResourcePolicy.BUILDER.getRuntimePasses().size());
  }

  @Test
  public void testDataSkewPolicy() {
    assertEquals(20, DataSkewPolicy.BUILDER.getCompileTimePasses().size());
    assertEquals(1, DataSkewPolicy.BUILDER.getRuntimePasses().size());
  }

  @Test
  public void testShouldFailPolicy() {
    try {
      final Policy failPolicy = new PolicyBuilder()
          .registerCompileTimePass(new TransientResourceCompositePass())
          .registerCompileTimePass(new DefaultScheduleGroupPass())
          .build();
    } catch (Exception e) { // throw an exception if default execution properties are not set.
      assertTrue(e instanceof CompileTimeOptimizationException);
      assertTrue(e.getMessage().contains("Prerequisite ExecutionProperty hasn't been met"));
    }
  }
}
