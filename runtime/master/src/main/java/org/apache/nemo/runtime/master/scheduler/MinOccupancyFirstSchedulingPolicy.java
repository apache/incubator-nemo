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
package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.Task;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.runtime.master.ExecutorRepresenter;
import org.apache.nemo.runtime.master.PlanStateManager;
import org.apache.nemo.runtime.master.TaskScheduledMapMaster;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This policy chooses a set of Executors, on which have minimum running Tasks.
 */
@ThreadSafe
@DriverSide
public final class MinOccupancyFirstSchedulingPolicy implements SchedulingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(MinOccupancyFirstSchedulingPolicy.class.getName());

  private final PlanStateManager planStateManager;
  private final TaskScheduledMapMaster taskScheduledMap;
  private final ExecutorRegistry executorRegistry;

  @Inject
  private MinOccupancyFirstSchedulingPolicy(final PlanStateManager planStateManager,
                                            final TaskScheduledMapMaster taskScheduledMapMaster,
                                            final ExecutorRegistry executorRegistry) {
    this.planStateManager = planStateManager;
    this.taskScheduledMap = taskScheduledMapMaster;
    this.executorRegistry = executorRegistry;
  }

  @Override
  public ExecutorRepresenter selectExecutor(final Collection<ExecutorRepresenter> executors, final Task task) {
    final String stageId = RuntimeIdManager.getStageIdFromTaskId(task.getTaskId());
    final List<StageEdge> incoming = planStateManager.getPhysicalPlan().getStageDAG().getIncomingEdgesOf(stageId)
      .stream().filter(edge -> edge.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.OneToOne))
      .collect(Collectors.toList());

    final List<ExecutorRepresenter> candidates =
      incoming.stream().filter(edge -> {
      final String srcTaskId = RuntimeIdManager.generateTaskId(edge.getSrc().getId(),
        RuntimeIdManager.getIndexFromTaskId(task.getTaskId()), 0);
        LOG.info("Scheduling candidate task for {}: {}", task.getTaskId(), srcTaskId);
      return (taskScheduledMap.isTaskScheduled(srcTaskId)
      && !taskScheduledMap.getTaskExecutorIdMap().get(srcTaskId).equals(
        taskScheduledMap.getPrevTaskExecutorIdMap().get(task.getTaskId())));
    })
      .map(edge -> {
        final String srcTaskId = RuntimeIdManager.generateTaskId(edge.getSrc().getId(),
          RuntimeIdManager.getIndexFromTaskId(task.getTaskId()), 0);
        final String executorId = taskScheduledMap.getTaskExecutorIdMap().get(srcTaskId);
        return executorRegistry.getExecutorRepresentor(executorId);
      }).collect(Collectors.toList());

    LOG.info("Task {} candidates for incoming edges size {}", task.getTaskId(), candidates.size());

    if (candidates.size() > 0) {
        final OptionalInt minOccupancy =
        candidates.stream()
        .map(executor -> executor.getNumOfRunningTasks())
        .mapToInt(i -> i).min();

      return candidates.stream()
        .filter(executor -> executor.getNumOfRunningTasks() == minOccupancy.getAsInt())
        .findFirst()
        .orElseThrow(() -> new RuntimeException("No such executor"));
    } else {

      final OptionalInt minOccupancy =
        executors.stream()
          .map(executor -> executor.getNumOfRunningTasks())
          .mapToInt(i -> i).min();

      if (!minOccupancy.isPresent()) {
        throw new RuntimeException("Cannot find min occupancy");
      }

      return executors.stream()
        .filter(executor -> executor.getNumOfRunningTasks() == minOccupancy.getAsInt())
        .findFirst()
        .orElseThrow(() -> new RuntimeException("No such executor"));
    }
  }
}
