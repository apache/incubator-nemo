package org.apache.nemo.runtime.lambdaexecutor.kafka;

import io.netty.buffer.ByteBuf;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;

import java.util.Map;

public final class KafkaOffloadingOutput {

  public final boolean moveToVMScaling;
  public final String taskId;
  public final int id;
  public final UnboundedSource.CheckpointMark checkpointMark;
  public final Map<String, GBKFinalState> stateMap;
  public final Map<String, Coder<GBKFinalState>> stateCoderMap;
  public final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder;

  public final ByteBuf byteBuf;

  public KafkaOffloadingOutput(
    final boolean moveToVMScaling,
    final String taskId,
    final int id,
    final UnboundedSource.CheckpointMark checkpointMark,
    final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder,
    final Map<String, GBKFinalState> stateMap,
    final Map<String, Coder<GBKFinalState>> stateCoderMap) {
    this.moveToVMScaling = moveToVMScaling;
    this.taskId = taskId;
    this.id = id;
    this.checkpointMark = checkpointMark;
    this.checkpointMarkCoder = checkpointMarkCoder;
    this.stateMap = stateMap;
    this.stateCoderMap = stateCoderMap;

    this.byteBuf = null;
  }

  public KafkaOffloadingOutput(final boolean moveToVMScaling,
                               final String taskId,
                               final ByteBuf byteBuf) {
    this.moveToVMScaling = moveToVMScaling;
    this.taskId = taskId;
    this.byteBuf = byteBuf;

    this.id = 0;
    this.checkpointMark = null;
    this.stateMap = null;
    this.stateCoderMap = null;
    this.checkpointMarkCoder = null;
  }

  @Override
  public String toString() {
    return "[KafkaOutput: " + taskId + "]";
  }
}
