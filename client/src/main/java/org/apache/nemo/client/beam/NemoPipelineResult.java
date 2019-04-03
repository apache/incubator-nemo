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

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.nemo.client.ClientEndpoint;
import org.apache.nemo.client.JobLauncher;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Beam result.
 */
public final class NemoPipelineResult extends ClientEndpoint implements PipelineResult {
  private static final Logger LOG = LoggerFactory.getLogger(NemoPipelineResult.class.getName());
  private final CountDownLatch jobDone;

  /**
   * Default constructor.
   */
  public NemoPipelineResult() {
    super(new BeamStateTranslator());
    this.jobDone = new CountDownLatch(1);
  }

  /**
   * Signal that the job is finished to the NemoPipelineResult object.
   */
  public void setJobDone() {
    this.jobDone.countDown();
  }

  @Override
  public State getState() {
    return (State) super.getPlanState();
  }

  @Override
  public State cancel() throws IOException {
    try {
      JobLauncher.shutdown();
      this.jobDone.await();
      return State.CANCELLED;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public State waitUntilFinish(final Duration duration) {
    try {
      if (duration.getMillis() < 1) {
        this.jobDone.await();
        return State.DONE;
      } else {
        final boolean finished = this.jobDone.await(duration.getMillis(), TimeUnit.MILLISECONDS);
        if (finished) {
          LOG.info("Job successfully finished before timeout of {}ms, while waiting until finish",
            duration.getMillis());
          return State.DONE;
        } else {
          LOG.warn("Job timed out before {}ms, while waiting until finish. Call 'cancel' to cancel the job.",
            duration.getMillis());
          return State.RUNNING;
        }
      }
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    // return (State) super.waitUntilJobFinish(duration.getMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public State waitUntilFinish() {
    return waitUntilFinish(Duration.ZERO);
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException("metrics() in frontend.beam.NemoPipelineResult");
  }
}
