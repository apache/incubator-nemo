package edu.snu.vortex.runtime.executor.datatransfer;

import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex;
import edu.snu.vortex.runtime.common.plan.physical.Task;
import edu.snu.vortex.runtime.exception.UnsupportedDataPlacementException;
import edu.snu.vortex.runtime.executor.dataplacement.*;

/**
 * A factory that produces {@link InputReader} and {@link OutputWriter}.
 * Each {@link DataTransfer} is bound to the {@link DataPlacement} mechanism specified by
 * the data placement attribute.
 */
public final class DataTransferFactory {
  /**
   * The different types of {@link DataPlacement} mechanisms supported by Runtime.
   */
  private LocalDataPlacement localDataPlacement;
  private MemoryDataPlacement memoryDataPlacement;
  private FileDataPlacement fileDataPlacement;
  private MemoryFileDataPlacement memoryFileDataPlacement;
  private DistributedStorageDataPlacement distributedStorageDataPlacement;

  public DataTransferFactory() {
    this.localDataPlacement = new LocalDataPlacement();
    this.memoryDataPlacement = new MemoryDataPlacement();
    this.fileDataPlacement = new FileDataPlacement();
    this.memoryFileDataPlacement = new MemoryFileDataPlacement();
    this.distributedStorageDataPlacement = new DistributedStorageDataPlacement();
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
    return new OutputWriter(srcTask, dstRuntimeVertex, runtimeEdge,
        getDataPlacement(runtimeEdge.getEdgeAttributes().get(RuntimeAttribute.Key.ChannelDataPlacement)));
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
    return new InputReader(dstTask, srcRuntimeVertex, runtimeEdge,
        getDataPlacement(runtimeEdge.getEdgeAttributes().get(RuntimeAttribute.Key.ChannelDataPlacement)));
  }

  private DataPlacement getDataPlacement(final RuntimeAttribute dataPlacementAttribute) {
    switch (dataPlacementAttribute) {
    case Local:
      return localDataPlacement;
    case Memory:
      return memoryDataPlacement;
    case File:
      return fileDataPlacement;
    case MemoryFile:
      return memoryFileDataPlacement;
    case DistributedStorage:
      return distributedStorageDataPlacement;
    default:
      throw new UnsupportedDataPlacementException(new Exception(dataPlacementAttribute + " is not supported."));
    }
  }
}
