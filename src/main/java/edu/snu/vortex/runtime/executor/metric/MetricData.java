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

import java.util.*;

/**
 * MetricData that holds executor side metrics.
 */
public class MetricData {

  private final Enum computationUnitEnum;
  private final String computationUnitId;
  private final String executorId;
  private final int stageScheduleAttemptIdx;
  private final String startState;
  private final String endState;
  private final long elapsedTime;

  public MetricData(final Enum computationUnit,
                    final String computationUnitId,
                    final String executorId,
                    final int stageScheduleAttemptIdx,
                    final String startState,
                    final String endState,
                    final long elapsedTime) {
    this.computationUnitEnum = computationUnit;
    this.computationUnitId = computationUnitId;
    this.executorId = executorId;
    this.stageScheduleAttemptIdx = stageScheduleAttemptIdx;
    this.startState = startState;
    this.endState = endState;
    this.elapsedTime = elapsedTime;
  }

  public final Enum getComputationUnit() {
    return computationUnitEnum; }
  public final String getComputationUnitId() {
    return computationUnitId;
  }
  public final String getExecutorId() {
    return executorId;
  }
  public final int getStageScheduleAttemptIdx() {
    return stageScheduleAttemptIdx;
  }
  public final String getStartState() {
    return startState;
  }
  public final String getEndState() {
    return endState;
  }
  public final long getElapsedTime() {
    return elapsedTime;
  }

  /**
   * Computation units to measure.
   */
  public enum ComputationUnit {
    JOB,
    STAGE,
    TASKGROUP,
    TASK
  }

  public final Map<String, Object> toJson() {
    final Map<String, Object> jsonMetricData = new HashMap<>();
    jsonMetricData.put(getComputationUnit().toString(), getComputationUnitId());
    jsonMetricData.put("Executor", getExecutorId());
    jsonMetricData.put("StageScheduleAttemptIdx", getStageScheduleAttemptIdx());
    jsonMetricData.put("StartState", getStartState());
    jsonMetricData.put("EndState", getEndState());
    jsonMetricData.put("ElapsedTime", getElapsedTime());

    return jsonMetricData;
  }
}
