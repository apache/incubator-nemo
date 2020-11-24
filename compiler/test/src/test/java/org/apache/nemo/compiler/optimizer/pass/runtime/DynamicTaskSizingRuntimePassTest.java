/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.compiler.optimizer.pass.runtime;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.MessageIdEdgeProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.compiler.CompilerTestUtil;
import org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping.LoopUnrollingPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping.SamplingTaskSizingPass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;

/**
 * Test {@link DynamicTaskSizingRuntimePass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class DynamicTaskSizingRuntimePassTest {
  private IRDAG compiledDAG;
  private final String mapKey = "opt.parallelism";
  private final Map<String, Long> message = new HashMap<>();

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileWordCountDAG();
    message.put(mapKey, 8L);
  }

  /**
   * Testing method of whether the runtime pass successfully changes the parallelism of target vertices.
   */
  @Test
  public void testDynamicTaskSizingRuntimePass() {
    final IRDAG dagAfterCompileTimePass =
      new LoopUnrollingPass().apply(new SamplingTaskSizingPass().apply(compiledDAG));
    final Set<IREdge> edgesToExamine = dagAfterCompileTimePass.getEdges().stream()
      .filter(irEdge -> irEdge.getExecutionProperties().containsKey(MessageIdEdgeProperty.class))
      .collect(Collectors.toSet());

    if (!edgesToExamine.isEmpty()) {
      final IRDAG afterRuntimePass = new DynamicTaskSizingRuntimePass()
        .apply(dagAfterCompileTimePass, new Message<>(1, edgesToExamine, message));
      for (IREdge edge : afterRuntimePass.getEdges())
        if (edgesToExamine.contains(edge)) {
          int dstParallelism = edge.getDst().getPropertyValue(ParallelismProperty.class).orElse(1);
          assertEquals(dstParallelism, 8);
        }
    }
  }
}
