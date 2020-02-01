package org.apache.nemo.runtime.lambdaexecutor;

import io.netty.buffer.ByteBuf;
import org.apache.beam.sdk.coders.Coder;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;

import java.util.Map;

public final class StateOutput {

  public final boolean moveToVmScaling;
  public String taskId;
  public final Map<String, GBKFinalState> stateMap;
  public final Map<String, Coder<GBKFinalState>> stateCoderMap;
  public final ByteBuf byteBuf;

  public StateOutput(final boolean moveToVmScaling,
                     final String taskId,
                     final Map<String, GBKFinalState> stateMap,
                     final Map<String, Coder<GBKFinalState>> stateCoderMap) {
    this.moveToVmScaling = moveToVmScaling;
    this.taskId = taskId;
    this.stateMap = stateMap;
    this.stateCoderMap = stateCoderMap;

    this.byteBuf = null;
  }

  public StateOutput(final boolean moveToVmScaling,
                     final String taskId,
                     final ByteBuf byteBuf) {
    this.moveToVmScaling = moveToVmScaling;
    this.taskId = taskId;
    this.byteBuf = byteBuf;

    this.stateMap = null;
    this.stateCoderMap = null;
  }
}
