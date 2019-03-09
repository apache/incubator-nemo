/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.client.beam;

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
