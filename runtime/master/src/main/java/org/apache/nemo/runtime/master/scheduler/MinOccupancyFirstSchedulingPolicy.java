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

  private void getO2ODownstreams(final String stageId, final Set<String> l) {
    final List<String> outgoing = planStateManager.getPhysicalPlan().getStageDAG().getOutgoingEdgesOf(stageId)
      .stream().filter(edge -> edge.getDataCommunicationPattern()
        .equals(CommunicationPatternProperty.Value.OneToOne) ||
        edge.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.TransientOneToOne))
      .map(edge -> edge.getDst().getId())
      .collect(Collectors.toList());

    l.addAll(outgoing);

    outgoing.forEach(edge -> {
      if (!l.contains(edge)) {
        getO2ODownstreams(edge, l);
      }
    });
  }

  private void getO2OUpstreams(final String stageId, final Set<String> l) {
    final List<String> incoming = planStateManager.getPhysicalPlan().getStageDAG().getIncomingEdgesOf(stageId)
      .stream().filter(edge -> edge.getDataCommunicationPattern()
        .equals(CommunicationPatternProperty.Value.OneToOne) ||
        edge.getDataCommunicationPattern().equals(CommunicationPatternProperty.Value.TransientOneToOne))
      .map(edge -> edge.getSrc().getId())
      .collect(Collectors.toList());


    incoming.forEach(stage -> {
      if (!l.contains(stage)) {
        l.add(stage);
        getO2OUpstreams(stage, l);
      }
    });
  }

  @Override
  public ExecutorRepresenter selectExecutor(final Collection<ExecutorRepresenter> executors, final Task task) {
    final String stageId = RuntimeIdManager.getStageIdFromTaskId(task.getTaskId());


    LOG.info("Candidate executors for scheudling task {}: {}",
      task.getTaskId(), executors);

    // For o2o-aware scheduling
    final Set<String> o2oEdges = new HashSet<>();
    getO2OUpstreams(stageId, o2oEdges);
    getO2ODownstreams(stageId, o2oEdges);

    final List<ExecutorRepresenter> candidates =
      o2oEdges.stream()
        // check whether the o2o source task is scheduled
        .filter(edge -> {
      final String srcTaskId = RuntimeIdManager.generateTaskId(edge,
        RuntimeIdManager.getIndexFromTaskId(task.getTaskId()), 0);
        LOG.info("Scheduling candidate task for {}: {}, srcSchedule: {}, srcExecutorId: {}," +
          "prevExecutorId: {}", task.getTaskId(), srcTaskId,
          taskScheduledMap.isTaskScheduled(srcTaskId),
          taskScheduledMap.getTaskExecutorIdMap().get(srcTaskId),
          taskScheduledMap.getPrevTaskExecutorIdMap().get(task.getTaskId()));

      return (taskScheduledMap.isTaskScheduled(srcTaskId)
      && !taskScheduledMap.getTaskExecutorIdMap().get(srcTaskId).equals(
        taskScheduledMap.getPrevTaskExecutorIdMap().get(task.getTaskId())));
    })
      .map(edge -> {
        final String srcTaskId = RuntimeIdManager.generateTaskId(edge,
          RuntimeIdManager.getIndexFromTaskId(task.getTaskId()), 0);
        final String executorId = taskScheduledMap.getTaskExecutorIdMap().get(srcTaskId);
        return executorRegistry.getExecutorRepresentor(executorId);
      })
       // filter if the locality-aware executor is in the executors
        .filter(candidate -> executors.stream().anyMatch(executor ->
          executor.getExecutorId().equals(candidate.getExecutorId())))
        .collect(Collectors.toList());

    LOG.info("Task {} candidates for incoming edges size {}", task.getTaskId(), candidates);

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
