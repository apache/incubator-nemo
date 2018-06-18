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
package edu.snu.nemo.tests.compiler.optimizer.pass.compiletime.composite;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.StageIdProperty;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultParallelismPass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultStagePartitioningPass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.DisaggregationEdgeDataStorePass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.ReviseInterStageEdgeDataStorePass;
import edu.snu.nemo.tests.compiler.CompilerTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link DisaggregationEdgeDataStorePass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class DisaggregationPassTest {
  private DAG<IRVertex, IREdge> compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileALSDAG();
  }

  @Test
  public void testDisaggregation() throws Exception {
    final DAG<IRVertex, IREdge> processedDAG =
        new DisaggregationEdgeDataStorePass().apply(
            new ReviseInterStageEdgeDataStorePass().apply(
                new DefaultStagePartitioningPass().apply(
                    new DefaultParallelismPass().apply(compiledDAG))));

    processedDAG.getTopologicalSort().forEach(irVertex -> {
      processedDAG.getIncomingEdgesOf(irVertex).forEach(edgeToMerger -> {
        if (DataCommunicationPatternProperty.Value.OneToOne
            .equals(edgeToMerger.getPropertyValue(DataCommunicationPatternProperty.class).get())
            && edgeToMerger.getSrc().getPropertyValue(StageIdProperty.class).get()
            .equals(edgeToMerger.getDst().getPropertyValue(StageIdProperty.class).get())) {
          assertEquals(DataStoreProperty.Value.MemoryStore, edgeToMerger.getPropertyValue(DataStoreProperty.class).get());
        } else {
          assertEquals(DataStoreProperty.Value.GlusterFileStore,
              edgeToMerger.getPropertyValue(DataStoreProperty.class).get());
        }
      });
    });
  }
}
