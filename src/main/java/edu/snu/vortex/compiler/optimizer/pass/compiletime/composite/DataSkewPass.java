/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.vortex.compiler.optimizer.pass.compiletime.composite;

import edu.snu.vortex.compiler.optimizer.pass.compiletime.annotating.DataSkewEdgeDataStorePass;
import edu.snu.vortex.compiler.optimizer.pass.compiletime.annotating.DataSkewEdgeMetricCollectionPass;
import edu.snu.vortex.compiler.optimizer.pass.compiletime.annotating.DataSkewVertexPass;
import edu.snu.vortex.compiler.optimizer.pass.compiletime.reshaping.DataSkewReshapingPass;

import java.util.Arrays;

/**
 * Pass to modify the DAG for a job to perform data skew.
 * It adds a {@link edu.snu.vortex.compiler.ir.MetricCollectionBarrierVertex} before ScatterGather edges, to make a
 * barrier before it, and to use the metrics to repartition the skewed data.
 * NOTE: we currently put the DataSkewPass at the end of the list for each policies, as it needs to take a snapshot at
 * the end of the pass. This could be prevented by modifying other passes to take the snapshot of the DAG at the end of
 * each passes for metricCollectionVertices.
 */
public final class DataSkewPass extends CompositePass {
  public static final String SIMPLE_NAME = "DataSkewPass";

  public DataSkewPass() {
    super(Arrays.asList(
        new DataSkewReshapingPass(),
        new DataSkewVertexPass(),
        new DataSkewEdgeDataStorePass(),
        new DataSkewEdgeMetricCollectionPass()
    ));
  }

  @Override
  public String getName() {
    return SIMPLE_NAME;
  }
}
