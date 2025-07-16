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
package org.apache.nemo.compiler.optimizer.pass.compiletime.composite;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.WorkStealingStateProperty;
import org.apache.nemo.compiler.CompilerTestUtil;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultParallelismPass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static junit.framework.TestCase.assertEquals;

/**
 * Test {@link WorkStealingCompositePass} with MR workload.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class WorkStealingCompositePassTest {
  private IRDAG mrDAG;

  @Before
  public void setUp() throws Exception {
  }

  @Test
  public void testWorkStealingPass() throws Exception {
    mrDAG = CompilerTestUtil.compileWordCountWorkStealingDAG();

    final IRDAG processedDAG = new WorkStealingCompositePass().apply(new DefaultParallelismPass().apply(mrDAG));

    int numSplitSVertex = 0;
    int numMergeVertex = 0;

    for (IRVertex vertex : processedDAG.getTopologicalSort()) {
      if (vertex.getPropertyValue(WorkStealingStateProperty.class).equals("SPLIT")) {
        numSplitSVertex++;
      } else if (vertex.getPropertyValue(WorkStealingStateProperty.class).equals("MERGE")) {
        numMergeVertex++;
      }
    }

    assertEquals(numSplitSVertex, numMergeVertex);
  }
}
