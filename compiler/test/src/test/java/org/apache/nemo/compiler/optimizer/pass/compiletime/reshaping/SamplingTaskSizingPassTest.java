package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.utility.TaskSizeSplitterVertex;
import org.apache.nemo.compiler.CompilerTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static junit.framework.TestCase.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class SamplingTaskSizingPassTest {
  private IRDAG compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileWordCountDAG();
  }

  @Test
  public void testSamplingTaskSizingPass() throws Exception {
    final IRDAG processedDAG = new SamplingTaskSizingPass().apply(compiledDAG);
    final int numberOfVerticesBeforePass = compiledDAG.getTopologicalSort().size();
    int numberOfVerticesAfterPass = 0;
    for (IRVertex vertex : processedDAG.getTopologicalSort()) {
      if (vertex instanceof TaskSizeSplitterVertex) {
        numberOfVerticesAfterPass += ((TaskSizeSplitterVertex) vertex).getOriginalVertices().size();
      } else {
        numberOfVerticesAfterPass++;
      }
    }
    assertEquals(numberOfVerticesBeforePass, numberOfVerticesAfterPass);
  }
}
