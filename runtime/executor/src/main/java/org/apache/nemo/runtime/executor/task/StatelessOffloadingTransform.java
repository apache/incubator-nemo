package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.apache.nemo.offloading.common.OffloadingTransform;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;

public final class StatelessOffloadingTransform<I, O> implements OffloadingTransform<I, O> {

  private final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag;

  public StatelessOffloadingTransform(final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag) {
    this.irVertexDag = irVertexDag;
  }

  @Override
  public void prepare(final OffloadingContext context,
                      final OffloadingOutputCollector<O> outputCollector) {

  }

  @Override
  public void onData(I element) {

  }

  @Override
  public void close() {

  }
}
