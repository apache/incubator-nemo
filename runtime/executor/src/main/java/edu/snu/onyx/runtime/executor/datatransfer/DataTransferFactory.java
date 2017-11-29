package edu.snu.onyx.runtime.executor.datatransfer;

import edu.snu.onyx.conf.JobConf;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.runtime.common.plan.RuntimeEdge;
import edu.snu.onyx.runtime.common.plan.physical.Task;
import edu.snu.onyx.runtime.executor.data.PartitionManagerWorker;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A factory that produces {@link InputReader} and {@link OutputWriter}.
 */
public final class DataTransferFactory {

  private final PartitionManagerWorker partitionManagerWorker;
  private final int hashRangeMultiplier;

  @Inject
  public DataTransferFactory(@Parameter(JobConf.HashRangeMultiplier.class) final int hashRangeMultiplier,
                             final PartitionManagerWorker partitionManagerWorker) {
    this.hashRangeMultiplier = hashRangeMultiplier;
    this.partitionManagerWorker = partitionManagerWorker;
  }

  /**
   * Creates an {@link OutputWriter}.
   *
   * @param srcTask          the {@link Task} that outputs the data to be written.
   * @param dstRuntimeVertex the {@link IRVertex} that will take the output data as its input.
   * @param runtimeEdge      that connects the srcTask to the tasks belonging to dstRuntimeVertex.
   * @return the {@link OutputWriter} created.
   */
  public OutputWriter createWriter(final Task srcTask,
                                   final IRVertex dstRuntimeVertex,
                                   final RuntimeEdge<?> runtimeEdge) {
    return new OutputWriter(hashRangeMultiplier, srcTask.getIndex(),
        srcTask.getRuntimeVertexId(), dstRuntimeVertex, runtimeEdge, partitionManagerWorker);
  }

  public OutputWriter createLocalWriter(final Task srcTask,
                                        final RuntimeEdge<?> runtimeEdge) {
    return createWriter(srcTask, null, runtimeEdge);
  }

  /**
   * Creates an {@link InputReader} between two stages.
   *
   * @param dstTask          the {@link Task} that takes the input data.
   * @param srcRuntimeVertex the {@link IRVertex} that output the data to be read.
   * @param runtimeEdge      that connects the tasks belonging to srcRuntimeVertex to dstTask.
   * @return the {@link InputReader} created.
   */
  public InputReader createReader(final Task dstTask,
                                  final IRVertex srcRuntimeVertex,
                                  final RuntimeEdge runtimeEdge) {
    return new InputReader(
        dstTask.getIndex(), dstTask.getTaskGroupId(), srcRuntimeVertex, runtimeEdge, partitionManagerWorker);
  }

  /**
   * Creates a local {@link InputReader} between two task in a single task group.
   *
   * @param dstTask          the {@link Task} that takes the input data.
   * @param runtimeEdge      that connects the tasks belonging to srcRuntimeVertex to dstTask.
   * @return the {@link InputReader} created.
   */
  public InputReader createLocalReader(final Task dstTask,
                                       final RuntimeEdge runtimeEdge) {
    return createReader(dstTask, null, runtimeEdge);
  }
}
