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
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.conf.EvalConf;
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

  private final Map<String, ExecutorRepresenter> lambdaPreScheduledExecutor;

  @Inject
  private MinOccupancyFirstSchedulingPolicy(final PlanStateManager planStateManager,
                                            final TaskScheduledMapMaster taskScheduledMapMaster,
                                            final ExecutorRegistry executorRegistry) {
    this.planStateManager = planStateManager;
    this.taskScheduledMap = taskScheduledMapMaster;
    this.executorRegistry = executorRegistry;
    this.lambdaPreScheduledExecutor = new HashMap<>();
  }


  @Override
  public ExecutorRepresenter selectExecutor(final Collection<ExecutorRepresenter> executors,
                                            final Task task) {

    // LOG.info("Candidate executors for scheudling task {}: {}",
    //  task.getTaskId(), executors);

    if (task.getPropertyValue(ResourcePriorityProperty.class).equals(ResourcePriorityProperty.LAMBDA)) {
      if (lambdaPreScheduledExecutor.containsKey(task.getTaskId())) {
        return lambdaPreScheduledExecutor.get(task.getTaskId());
      }
    }

    final Set<String> scheduledTasks = executors.stream()
      .map(e -> e.getScheduledTasks())
      .reduce((s1, s2) -> {
        final Set<Task> s = new HashSet<Task>(s1);
        s.addAll(s2);
        return s;
      }).get()
      .stream()
      .map(t -> t.getTaskId())
      .collect(Collectors.toSet());

    final Map<String, ExecutorRepresenter> scheduledExecutors = new HashMap<>();
    executors.forEach(executor -> {
      executor.getScheduledTasks().forEach(t -> {
        scheduledExecutors.put(t.getTaskId(), executor);
      });
    });

    final List<ExecutorRepresenter> candidates =
      task.getO2oStages().stream()
        // check whether the o2o source task is scheduled
        .filter(o2oStage -> {
      final String srcTaskId = RuntimeIdManager.generateTaskId(o2oStage,
        RuntimeIdManager.getIndexFromTaskId(task.getTaskId()), 0);
//        LOG.info("Scheduling candidate task for {}: {}, srcSchedule: {}, srcExecutorId: {}," +
//          "prevExecutorId: {}", task.getTaskId(), srcTaskId,
//          scheduledTasks.contains(srcTaskId),
//          scheduledTasks.contains(srcTaskId) ? scheduledExecutors.get(srcTaskId).getExecutorId() : "null",
//          taskScheduledMap.getPrevTaskExecutorIdMap().get(task.getTaskId()));

      return (scheduledTasks.contains(srcTaskId)
      && !scheduledExecutors.get(srcTaskId).getExecutorId().equals(
        taskScheduledMap.getPrevTaskExecutorIdMap().get(task.getTaskId())));
    })
      .map(edge -> {
        final String srcTaskId = RuntimeIdManager.generateTaskId(edge,
          RuntimeIdManager.getIndexFromTaskId(task.getTaskId()), 0);
        final String executorId = scheduledExecutors.get(srcTaskId).getExecutorId();
        return executorRegistry.getExecutorRepresentor(executorId);
      })
       // filter if the locality-aware executor is in the executors
        .filter(candidate -> executors.stream().anyMatch(executor ->
          executor.getExecutorId().equals(candidate.getExecutorId())))
        .collect(Collectors.toList());

   // LOG.info("Task {} candidates for incoming edges size {}", task.getTaskId(), candidates);

    if (candidates.size() > 0) {
      // O2O locality-aware
        final OptionalInt minOccupancy =
        candidates.stream()
        .map(executor -> executor.getScheduledTasks().size())
        .mapToInt(i -> i).min();

   //   LOG.info("O2o-aware scheduling task {} to {}", task.getTaskId(), candidates);

      final ExecutorRepresenter er = candidates.stream()
        .filter(executor -> executor.getScheduledTasks().size() == minOccupancy.getAsInt())
        .findFirst()
        .orElseThrow(() -> new RuntimeException("No such executor"));

      if (er.getContainerType().equals(ResourcePriorityProperty.LAMBDA)) {
        lambdaPreScheduledExecutor.put(task.getTaskId(), er);
      }

      return er;

    } else {
      // avoid allocating same stage tasks in the same executor as much as possible.
      final List<ExecutorRepresenter> nonConflictExecutors = executors.stream()
        .filter(executor -> executor.getScheduledTasks()
          .stream().map(t -> RuntimeIdManager.getStageIdFromTaskId(t.getTaskId()))
          .noneMatch(sid ->
            task.getO2oStages().contains(sid) ||
            sid.equals(RuntimeIdManager.getStageIdFromTaskId(task.getTaskId()))))
        .collect(Collectors.toList());

      if (nonConflictExecutors.size() > 0) {
        // There are executors that have no conflicting stages (o2o stages)
        // Select min occupancy
        final OptionalLong minOccupancy = nonConflictExecutors.stream()
          .map(executor -> executor.getScheduledTasks().size())
          .mapToLong(i -> i).min();

        final ExecutorRepresenter er = nonConflictExecutors.stream()
          .filter(executor -> executor.getScheduledTasks().size() == minOccupancy.getAsLong())
          .findFirst()
          .orElseThrow(() -> new RuntimeException("No such executor"));


        if (er.getContainerType().equals(ResourcePriorityProperty.LAMBDA)) {
          lambdaPreScheduledExecutor.put(task.getTaskId(), er);
        }

        return er;

      } else {
        // Avoid allocating same stage tasks in the same executor as much as possible.
        final OptionalLong minOccupancy = executors.stream()
          .map(executor ->
            executor.getScheduledTasks().stream()
              .filter(t -> t.getStageId().equals(task.getStageId()))
              .count())
          .mapToLong(i -> i).min();

        final ExecutorRepresenter er = executors.stream()
        .filter(executor -> (executor.getScheduledTasks().stream()
          .filter(t -> t.getStageId().equals(task.getStageId())).count()) == minOccupancy.getAsLong())
        .findFirst()
        .orElseThrow(() -> new RuntimeException("No such executor"));


        if (er.getContainerType().equals(ResourcePriorityProperty.LAMBDA)) {
          lambdaPreScheduledExecutor.put(task.getTaskId(), er);
        }

        return er;
      }
    }
  }
}
