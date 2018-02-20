package edu.snu.nemo.runtime.executor.datatransfer;

import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.common.plan.physical.Task;
import edu.snu.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * A factory that produces {@link InputReader} and {@link OutputWriter}.
 */
public final class DataTransferFactory {

  private final BlockManagerWorker blockManagerWorker;
  private final int hashRangeMultiplier;

  @Inject
  public DataTransferFactory(@Parameter(JobConf.HashRangeMultiplier.class) final int hashRangeMultiplier,
                             final BlockManagerWorker blockManagerWorker) {
    this.hashRangeMultiplier = hashRangeMultiplier;
    this.blockManagerWorker = blockManagerWorker;
  }

  /**
   * Creates an {@link OutputWriter} between two stages.
   *
   * @param srcTask     the {@link Task} that outputs the data to be written.
   * @param srcTaskIdx  the index of the source task.
   * @param dstIRVertex the {@link IRVertex} that will take the output data as its input.
   * @param runtimeEdge that connects the srcTask to the tasks belonging to dstIRVertex.
   * @return the {@link OutputWriter} created.
   */
  public OutputWriter createWriter(final Task srcTask,
                                   final int srcTaskIdx,
                                   // TODO #717: Remove nullable.
                                   // (If the destination is not an IR vertex, do not make OutputWriter.)
                                   @Nullable final IRVertex dstIRVertex,
                                   final RuntimeEdge<?> runtimeEdge) {
    return new OutputWriter(hashRangeMultiplier, srcTaskIdx,
        srcTask.getIrVertexId(), dstIRVertex, runtimeEdge, blockManagerWorker);
  }

  /**
   * Creates an local {@link OutputWriter} between two task in a single task group.
   *
   * @param srcTask     the {@link Task} that outputs the data to be written.
   * @param srcTaskIdx  the index of the source task.
   * @param runtimeEdge that connects the srcTask to the tasks belonging to dstIRVertex.
   * @return the {@link OutputWriter} created.
   */
  public OutputWriter createLocalWriter(final Task srcTask,
                                        final int srcTaskIdx,
                                        final RuntimeEdge<?> runtimeEdge) {
    return createWriter(srcTask, srcTaskIdx, null, runtimeEdge);
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
                                  // TODO #717: Remove nullable.
                                  // (If the source is not an IR vertex, do not make InputReader.)
                                  @Nullable final IRVertex srcIRVertex,
                                  final RuntimeEdge runtimeEdge) {
    return new InputReader(dstTaskIdx, srcIRVertex, runtimeEdge, blockManagerWorker);
  }

  /**
   * Creates a local {@link InputReader} between two task in a single task group.
   *
   * @param dstTaskIdx  the index of the destination task.
   * @param runtimeEdge that connects the tasks belonging to srcRuntimeVertex to dstTask.
   * @return the {@link InputReader} created.
   */
  public InputReader createLocalReader(final int dstTaskIdx,
                                       final RuntimeEdge runtimeEdge) {
    return createReader(dstTaskIdx, null, runtimeEdge);
  }
}
