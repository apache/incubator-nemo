/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.compiler.frontend.beam;

import edu.snu.nemo.client.StateTranslator;
import edu.snu.nemo.runtime.common.state.JobState;

import static org.apache.beam.sdk.PipelineResult.State.*;

/**
 * A {@link edu.snu.nemo.client.StateTranslator} for Beam.
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
  public Enum translateState(final JobState.State jobState) {
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
