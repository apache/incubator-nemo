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
package edu.snu.nemo.client;

import edu.snu.nemo.runtime.common.state.JobState;

/**
 * A class provides the translation between the state of job and corresponding
 * {@link ClientEndpoint}.
 */
public interface StateTranslator {

  /**
   * Translate a job state of nemo to a corresponding client endpoint state.
   *
   * @param jobState to translate.
   * @return the translated state.
   */
  Enum translateState(final JobState.State jobState);
}
