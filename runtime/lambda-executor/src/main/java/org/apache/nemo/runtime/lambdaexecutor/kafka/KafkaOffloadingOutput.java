package org.apache.nemo.runtime.lambdaexecutor.kafka;

import org.apache.beam.sdk.io.UnboundedSource;

public final class KafkaOffloadingOutput {

  public final String taskId;
  public final int id;
  public final UnboundedSource.CheckpointMark checkpointMark;

  public KafkaOffloadingOutput(
    final String taskId,
    final int id,
    final UnboundedSource.CheckpointMark checkpointMark) {
    this.taskId = taskId;
    this.id = id;
    this.checkpointMark = checkpointMark;
  }
}
