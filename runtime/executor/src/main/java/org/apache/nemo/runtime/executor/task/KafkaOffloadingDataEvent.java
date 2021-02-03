package org.apache.nemo.runtime.executor.task;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.offloading.common.DeprecatedOffloadingWorker;
import org.apache.nemo.runtime.executor.common.SourceVertexDataFetcher;

public final class KafkaOffloadingDataEvent {
  public final DeprecatedOffloadingWorker offloadingWorker;
  public final UnboundedSource unboundedSource;
  public final int id;
  public final SourceVertexDataFetcher sourceVertexDataFetcher;
  public final UnboundedSource.CheckpointMark checkpointMark;

  public KafkaOffloadingDataEvent(final DeprecatedOffloadingWorker offloadingWorker,
                                  final UnboundedSource unboundedSource,
                                  final int id,
                                  final SourceVertexDataFetcher sourceVertexDataFetcher,
                                  final UnboundedSource.CheckpointMark checkpointMark) {
    this.offloadingWorker = offloadingWorker;
    this.unboundedSource = unboundedSource;
    this.id = id;
    this.sourceVertexDataFetcher = sourceVertexDataFetcher;
    this.checkpointMark = checkpointMark;
  }
}
