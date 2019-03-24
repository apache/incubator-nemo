package org.apache.nemo.runtime.lambdaexecutor.kafka;

import org.apache.beam.sdk.io.UnboundedSource;

public final class KafkaOffloadingInput {

  public final int id;
  public final UnboundedSource.CheckpointMark checkpointMark;
  public final UnboundedSource unboundedSource;

  public KafkaOffloadingInput(
    final int id,
    final UnboundedSource.CheckpointMark checkpointMark,
    final UnboundedSource unboundedSource) {
    this.id = id;
    this.checkpointMark = checkpointMark;
    this.unboundedSource = unboundedSource;
  }
}
