package edu.snu.vortex.runtime.executor.datatransfer;

import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex;
import edu.snu.vortex.runtime.common.plan.physical.Task;
import edu.snu.vortex.runtime.executor.block.BlockManagerWorker;

import javax.inject.Inject;

/**
 * A factory that produces {@link InputReader} and {@link OutputWriter}.
 */
public final class DataTransferFactory {

  private final BlockManagerWorker blockManagerWorker;

  @Inject
  public DataTransferFactory(final BlockManagerWorker blockManagerWorker) {
    this.blockManagerWorker = blockManagerWorker;
  }

  /**
   * Creates an {@link OutputWriter}.
   * @param srcTask the {@link Task} that outputs the data to be written.
   * @param dstRuntimeVertex the {@link RuntimeVertex} that will take the output data as its input.
   * @param runtimeEdge that connects the srcTask to the tasks belonging to dstRuntimeVertex.
   * @return the {@link OutputWriter} created.
   */
  public OutputWriter createWriter(final Task srcTask,
                                   final RuntimeVertex dstRuntimeVertex,
                                   final RuntimeEdge runtimeEdge) {
    return new OutputWriter(srcTask.getIndex(), dstRuntimeVertex, runtimeEdge, blockManagerWorker);
  }

  public OutputWriter createWriter(final Task srcTask,
                                   final RuntimeEdge runtimeEdge) {
    return createWriter(srcTask, null, runtimeEdge);
  }

  /**
   * Creates an {@link InputReader}.
   * @param dstTask the {@link Task} that takes the input data.
   * @param srcRuntimeVertex the {@link RuntimeVertex} that output the data to be read.
   * @param runtimeEdge that connects the tasks belonging to srcRuntimeVertex to dstTask.
   * @return the {@link InputReader} created.
   */
  public InputReader createReader(final Task dstTask,
                                  final RuntimeVertex srcRuntimeVertex,
                                  final RuntimeEdge runtimeEdge) {
    return new InputReader(dstTask.getIndex(), srcRuntimeVertex, runtimeEdge, blockManagerWorker);
  }

  public InputReader createReader(final Task dstTask,
                                  final RuntimeEdge runtimeEdge) {
    return createReader(dstTask, null, runtimeEdge);
  }
}
