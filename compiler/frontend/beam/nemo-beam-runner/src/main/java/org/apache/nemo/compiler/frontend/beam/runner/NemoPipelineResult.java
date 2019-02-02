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
package org.apache.nemo.compiler.frontend.beam.runner;

import org.apache.beam.sdk.metrics.*;
import org.apache.nemo.client.ClientEndpoint;
import org.apache.beam.sdk.PipelineResult;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;

/**
 * Beam result.
 */
public final class NemoPipelineResult extends ClientEndpoint implements PipelineResult {

  /**
   * Default constructor.
   */
  public NemoPipelineResult() {
    super(new BeamStateTranslator());
  }

  @Override
  public State getState() {
    return (State) super.getPlanState();
  }

  @Override
  public State cancel() throws IOException {
    throw new UnsupportedOperationException("cancel() in frontend.beam.NemoPipelineResult");
  }

  @Override
  public State waitUntilFinish(final Duration duration) {
    // TODO #208: NemoPipelineResult#waitUntilFinish hangs
    // Previous code that hangs the job:
    // return (State) super.waitUntilJobFinish(duration.getMillis(), TimeUnit.MILLISECONDS);
    try {
      Thread.sleep(duration.getMillis());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return State.DONE;
  }

  @Override
  public State waitUntilFinish() {
    return waitUntilFinish(Duration.ZERO);
  }

  @Override
  public MetricResults metrics() {
    return new MetricResults() {
      @Override
      public MetricQueryResults queryMetrics(@Nullable final MetricsFilter filter) {
        return new MetricQueryResults() {
          @Override
          public Iterable<MetricResult<Long>> getCounters() {
            return Collections.emptyList();
          }

          @Override
          public Iterable<MetricResult<DistributionResult>> getDistributions() {
            return Collections.emptyList();
          }

          @Override
          public Iterable<MetricResult<GaugeResult>> getGauges() {
            return Collections.emptyList();
          }
        };
      }
    };
  }
}
