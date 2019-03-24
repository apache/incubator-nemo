package org.apache.nemo.runtime.lambdaexecutor.kafka;

import org.apache.beam.sdk.io.UnboundedSource;

public final class KafkaOffloadingOutput {

  public final int id;
  public final UnboundedSource.CheckpointMark checkpointMark;

  public KafkaOffloadingOutput(
    final int id,
    final UnboundedSource.CheckpointMark checkpointMark) {
    this.id = id;
    this.checkpointMark = checkpointMark;
  }
}
