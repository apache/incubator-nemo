package org.apache.nemo.runtime.lambdaexecutor;

import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;

import java.util.Map;

public final class StateOutput {

  public String taskId;
  public final Map<String, GBKFinalState> stateMap;

  public StateOutput(final String taskId,
                     final Map<String, GBKFinalState> stateMap) {
    this.taskId = taskId;
    this.stateMap = stateMap;
  }
}
