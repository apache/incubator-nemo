package org.apache.nemo.runtime.lambdaexecutor;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.apache.nemo.offloading.common.OffloadingTransform;

public class EmptyOffloadingTransform implements OffloadingTransform {
  private final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag;

  public EmptyOffloadingTransform(final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag) {
    this.irVertexDag = irVertexDag;
  }

  @Override
  public void prepare(OffloadingContext context, OffloadingOutputCollector outputCollector) {

  }

  @Override
  public void onData(Object element) {

  }

  @Override
  public void close() {

  }
}
