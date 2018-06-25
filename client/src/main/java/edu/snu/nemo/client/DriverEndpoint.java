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
import edu.snu.nemo.runtime.master.JobStateManager;

import java.util.concurrent.TimeUnit;

/**
 * A request endpoint in driver side of a job.
 */
public final class DriverEndpoint {

  /**
   * The {@link JobStateManager} of the running job.
   */
  private final JobStateManager jobStateManager;

  /**
   * The {@link ClientEndpoint} of the job.
   */
  private final ClientEndpoint clientEndpoint;

  /**
   * Construct an endpoint in driver side.
   * This method will be called by {@link ClientEndpoint}.
   * @param jobStateManager of running job.
   * @param clientEndpoint of running job.
   */
  public DriverEndpoint(final JobStateManager jobStateManager,
                        final ClientEndpoint clientEndpoint) {
    this.jobStateManager = jobStateManager;
    this.clientEndpoint = clientEndpoint;
    clientEndpoint.connectDriver(this);
  }

  /**
   * Get the current state of the running job.
   * This method will be called by {@link ClientEndpoint}.
   * @return the current state of the running job.
   */
  JobState.State getState() {
    return jobStateManager.getJobState();
  }

  /**
   * Wait for this job to be finished and return the final state.
   * It wait for at most the given time.
   * This method will be called by {@link ClientEndpoint}.
   * @param timeout of waiting.
   * @param unit of the timeout.
   * @return the final state of this job.
   */
  JobState.State waitUntilFinish(final long timeout,
                                 final TimeUnit unit) {
    return jobStateManager.waitUntilFinish(timeout, unit);
  }

  /**
   * Wait for this job to be finished and return the final state.
   * This method will be called by {@link ClientEndpoint}.
   * @return the final state of this job.
   */
  JobState.State waitUntilFinish() {
    return jobStateManager.waitUntilFinish();
  }
}
