package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.conf.JobConf;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A factory that produces {@link InputReader} and {@link OutputWriter}.
 */
public final class DataTransferFactory {

  private final BlockManagerWorker blockManagerWorker;
  private final int hashRangeMultiplier;

  @Inject
  private DataTransferFactory(@Parameter(JobConf.HashRangeMultiplier.class) final int hashRangeMultiplier,
                              final BlockManagerWorker blockManagerWorker) {
    this.hashRangeMultiplier = hashRangeMultiplier;
    this.blockManagerWorker = blockManagerWorker;
  }

  /**
   * Creates an {@link OutputWriter} between two stages.
   *
   * @param srcTaskId   the id of the source task.
   * @param dstIRVertex the {@link IRVertex} that will take the output data as its input.
   * @param runtimeEdge that connects the srcTask to the tasks belonging to dstIRVertex.
   * @return the {@link OutputWriter} created.
   */
  public OutputWriter createWriter(final String srcTaskId,
                                   final IRVertex dstIRVertex,
                                   final RuntimeEdge<?> runtimeEdge) {
    return new OutputWriter(hashRangeMultiplier, srcTaskId, dstIRVertex, runtimeEdge, blockManagerWorker);
  }

  /**
   * Creates an {@link InputReader} between two stages.
   *
   * @param dstTaskIdx  the index of the destination task.
   * @param srcIRVertex the {@link IRVertex} that output the data to be read.
   * @param runtimeEdge that connects the tasks belonging to srcIRVertex to dstTask.
   * @return the {@link InputReader} created.
   */
  public InputReader createReader(final int dstTaskIdx,
                                  final IRVertex srcIRVertex,
                                  final RuntimeEdge runtimeEdge) {
    return new InputReader(dstTaskIdx, srcIRVertex, runtimeEdge, blockManagerWorker);
  }
}
