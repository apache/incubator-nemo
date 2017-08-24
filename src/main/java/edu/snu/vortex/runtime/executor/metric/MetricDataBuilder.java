/*
 * Copyright (C) 2017 Seoul National University
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

package edu.snu.vortex.runtime.executor.metric;

/**
 * MetricData Builder.
 */
public final class MetricDataBuilder {

  private final Enum computationUnitEnum;
  private final String computationUnitId;
  private final String executorId;
  private int scheduleAttemptIdx;
  private String fromState;
  private String toState;
  private long startAt;
  private long endAt;

  public MetricDataBuilder(final Enum computationUnit,
                    final String computationUnitId,
                    final String executorId) {
    this.computationUnitEnum = computationUnit;
    this.computationUnitId = computationUnitId;
    this.executorId = executorId;
    this.scheduleAttemptIdx = 0;
    this.fromState = null;
    this.toState = null;
    this.startAt = 0;
    this.endAt = 0;
  }

  public void beginMeasurement(final int attemptIdx, final String startState, final long beginTimestamp) {
    this.scheduleAttemptIdx = attemptIdx;
    this.fromState = startState;
    this.startAt = beginTimestamp;
  }

  public void endMeasurement(final String endState, final long endTimestamp) {
    this.toState = endState;
    this.endAt = endTimestamp;
  }

  public Enum getComputationUnit() {
    return computationUnitEnum;
  }
  public String getComputationUnitId() {
    return computationUnitId;
  }
  public String getExecutorId() {
    return executorId;
  }
  public int getScheduleAttemptIdx() {
    return scheduleAttemptIdx;
  }
  public String getFromState() {
    return fromState;
  }
  public String getToState() {
    return toState;
  }
  public long getElapsedTime() {
    return endAt - startAt;
  }

  /**
   * Builds immutable MetricData.
   * @return the MetricData constructed by the builder.
   */
  public MetricData build() {
    return new MetricData(getComputationUnit(), getComputationUnitId(), getExecutorId(),
        getScheduleAttemptIdx(), getFromState(), getToState(), getElapsedTime());
  }

}
