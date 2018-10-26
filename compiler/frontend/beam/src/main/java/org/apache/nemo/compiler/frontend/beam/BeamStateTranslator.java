package org.apache.nemo.compiler.frontend.beam;

import org.apache.nemo.client.StateTranslator;
import org.apache.nemo.runtime.common.state.PlanState;

import static org.apache.beam.sdk.PipelineResult.State.*;

/**
 * A {@link org.apache.nemo.client.StateTranslator} for Beam.
 * It provides the translation between the state of job and Beam pipeline.
 */
public final class BeamStateTranslator implements StateTranslator {

  /**
   * Translate a job state of nemo to a corresponding Beam state.
   *
   * @param jobState to translate.
   * @return the translated state.
   */
  @Override
  public Enum translateState(final PlanState.State jobState) {
    switch (jobState) {
      case READY:
        return RUNNING;
      case EXECUTING:
        return RUNNING;
      case COMPLETE:
        return DONE;
      case FAILED:
        return FAILED;
      default:
        throw new UnsupportedOperationException("Unsupported job state.");
    }
  }
}
