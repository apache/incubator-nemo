package org.apache.nemo.common;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;

import java.util.List;
import java.util.Map;

public class TaskCachingElement {
  public final List<StageEdge> taskIncomingEdges;
  public final List<StageEdge> taskOutgoingEdges;
  public final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag;

  public TaskCachingElement(DAG<IRVertex, RuntimeEdge<IRVertex>> irDag,
                            final List<StageEdge> taskIncomingEdges,
                            final List<StageEdge> taskOutgoingEdges) {
    this.taskIncomingEdges = taskIncomingEdges;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.irDag = irDag;
  }
}
