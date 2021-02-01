package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.vertex.IRVertex;

import java.util.List;
import java.util.Map;

public interface OutputCollectorGenerator {

  OutputCollector generate(final IRVertex irVertex,
           final String taskId,
           final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
           final TaskExecutor taskExecutor,
           final SerializerManager serializerManager,
           final Map<String, Double> samplingMap,
           final Map<String, Pair<OperatorMetricCollector, OutputCollector>> vertexIdAndCollectorMap,
           final TaskMetrics taskMetrics,
           final List<StageEdge> outgoingEdges,
           final Map<String, NextIntraTaskOperatorInfo> operatorInfoMap);
}
