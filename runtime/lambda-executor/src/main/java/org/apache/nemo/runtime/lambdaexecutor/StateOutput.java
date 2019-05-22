package org.apache.nemo.runtime.lambdaexecutor;

import org.apache.beam.sdk.coders.Coder;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;

import java.util.Map;

public final class StateOutput {

  public String taskId;
  public final Map<String, GBKFinalState> stateMap;
  public final Map<String, Coder<GBKFinalState>> stateCoderMap;

  public StateOutput(final String taskId,
                     final Map<String, GBKFinalState> stateMap,
                     final Map<String, Coder<GBKFinalState>> stateCoderMap) {
    this.taskId = taskId;
    this.stateMap = stateMap;
    this.stateCoderMap = stateCoderMap;
  }
}
