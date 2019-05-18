package org.apache.nemo.runtime.lambdaexecutor;

import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;

public final class StateOutput {

  public String taskId;
  public final GBKFinalState state;

  public StateOutput(final String taskId,
                     final GBKFinalState state) {
    this.taskId = taskId;
    this.state = state;
  }
}
