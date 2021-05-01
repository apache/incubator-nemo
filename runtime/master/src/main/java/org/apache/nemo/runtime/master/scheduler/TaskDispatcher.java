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
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.common.Task;
import org.apache.nemo.common.TaskState;
import org.apache.nemo.runtime.master.*;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.reef.annotations.audience.DriverSide;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.EXECUTOR_MESSAGE_LISTENER_ID;

/**
 * Dispatches tasks to executors in discrete batches (dispatch iterations).
 * A dispatch iteration occurs under one of the following conditions
 * - An executor slot becomes available (for reasons such as task completion/failure, or executor addition)
 * - A new list of tasks become available (for reasons such as stage completion, task failure, or executor removal)
 */
@DriverSide
@NotThreadSafe
public final class TaskDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(TaskDispatcher.class.getName());
  private final PendingTaskCollectionPointer pendingTaskCollectionPointer;
  private final ExecutorService dispatcherThread;
  private final PlanStateManager planStateManager;
  private boolean isSchedulerRunning;
  private boolean isTerminated;

  private final DelayedSignalingCondition schedulingIteration = new DelayedSignalingCondition();
  private final ExecutorRegistry executorRegistry;
  private final SchedulingConstraintRegistry schedulingConstraintRegistry;
  private final SchedulingPolicy schedulingPolicy;
  private final TaskScheduledMapMaster taskScheduledMap;
  private final RendevousServer rendevousServer;
  private final ResourceRequestCounter resourceRequestCounter;

  private boolean reclaiming;

  private final Set<String> filteredOutExecutors;

  @Inject
  private TaskDispatcher(final SchedulingConstraintRegistry schedulingConstraintRegistry,
                         final SchedulingPolicy schedulingPolicy,
                         final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                         final ExecutorRegistry executorRegistry,
                         final PlanStateManager planStateManager,
                         final ResourceRequestCounter resourceRequestCounter,
                         final TaskScheduledMapMaster taskScheduledMap,
                         final RendevousServer rendevousServer) {
    this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
    this.dispatcherThread = Executors.newSingleThreadExecutor(runnable ->
        new Thread(runnable, "TaskDispatcher thread"));
    this.planStateManager = planStateManager;
    this.isSchedulerRunning = false;
    this.isTerminated = false;
    this.executorRegistry = executorRegistry;
    this.resourceRequestCounter = resourceRequestCounter;
    this.schedulingPolicy = schedulingPolicy;
    this.schedulingConstraintRegistry = schedulingConstraintRegistry;
    this.taskScheduledMap = taskScheduledMap;
    this.rendevousServer = rendevousServer;
    this.reclaiming = false;
    this.filteredOutExecutors = new HashSet<>();
  }

  public void setFilteredOutExecutors(final Set<String> filteredOutExecutors) {
    LOG.info("Set filtered out executors {}", filteredOutExecutors);
    this.filteredOutExecutors.clear();
    this.filteredOutExecutors.addAll(filteredOutExecutors);
  }

  public void setReclaiming(boolean rec) {
    this.reclaiming = rec;
  }

  /**
   * A separate thread is run to dispatch tasks to executors.
   * See comments in the {@link Scheduler} for avoiding race conditions.
   */
  private final class TaskDispatcherThread implements Runnable {
    @Override
    public void run() {

      VMScalingAddresses.check();

      // initialSetup();

      while (!isTerminated) {
        doScheduleTaskList();
       //  schedulingIteration.await();
      }

      if (planStateManager.isPlanDone()) {
        LOG.info("{} is complete.", planStateManager.getPlanId());
      } else {
        LOG.info("{} is incomplete.", planStateManager.getPlanId());
      }
      LOG.info("TaskDispatcher Terminated!");
    }
  }

  private List<List<Task>> getStageTasks(final Collection<Task> tasks) {
    final List<Task> tl = new ArrayList<>(tasks);
    tl.sort(new Comparator<Task>() {
      @Override
      public int compare(Task o1, Task o2) {
        return -RuntimeIdManager.getStageIdFromTaskId(o1.getTaskId())
          .compareTo(RuntimeIdManager.getStageIdFromTaskId(o2.getTaskId()));
      }
    });

    final List<List<Task>> stageTasks = new ArrayList<>();

    String prevStageId = "";
    for (final Task task : tl) {
      final String stageId = RuntimeIdManager.getStageIdFromTaskId(task.getTaskId());
      if (!stageId.equals(prevStageId)) {
        stageTasks.add(new ArrayList<>());
      }

      final List<Task> st = stageTasks.get(stageTasks.size()-1);
      st.add(task);

      prevStageId = stageId;
    }

    return stageTasks;
  }


  private void doScheduleTaskList() {
      final List<Task> taskList = pendingTaskCollectionPointer.getTasks();
      if (taskList == null) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return;
      }
      /* final Optional<Collection<Task>> taskListOptional = pendingTaskCollectionPointer.getAndSetNull();
      if (!taskListOptional.isPresent()) {
        // Task list is empty
        LOG.debug("PendingTaskCollectionPointer is empty. Awaiting for more Tasks...");
        return;
      }

      final Collection<Task> taskList = taskListOptional.get();
      */

    // Check whehter multiple containers are created (for scheduling)
    // !all container is executed
    if (resourceRequestCounter.resourceRequestCount.get() > 0) {
      pendingTaskCollectionPointer.addTasks(taskList);
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return;
    }

      LOG.info("Size of tasks: {}", taskList.size());
      // Reverse order by stage number
      final List<List<Task>> stageTasks = getStageTasks(taskList);

      final List<Task> couldNotSchedule = new ArrayList<>();

      for (final List<Task> stageTask : stageTasks) {
        for (final Task task : stageTask) {
          if (!planStateManager.getTaskState(task.getTaskId()).equals(TaskState.State.READY)) {
            // Guard against race conditions causing duplicate task launches
            LOG.debug("Skipping {} as it is not READY", task.getTaskId());
            continue;
          }

          LOG.info("Start to scheduling task {}, resource {}", task,
            task.getExecutionProperties().get(ResourcePriorityProperty.class));

          executorRegistry.viewExecutors(executors -> {
            final MutableObject<Set<ExecutorRepresenter>> candidateExecutors = new MutableObject<>(executors);
            task.getExecutionProperties().forEachProperties(property -> {
              final Optional<SchedulingConstraint> constraint = schedulingConstraintRegistry.get(property.getClass());
              if (constraint.isPresent() && !candidateExecutors.getValue().isEmpty()) {
                candidateExecutors.setValue(candidateExecutors.getValue().stream()
                  .filter(e -> constraint.get().testSchedulability(e, task))
                  .collect(Collectors.toSet()));
              }
            });

            if (reclaiming) {
              // DEPRECATED !!!!
              // DEPRECATED !!!!
              // DEPRECATED !!!!
              final ExecutorRepresenter selectedExecutor =
                executorRegistry.getExecutorRepresentor(
                  taskScheduledMap.getTaskOriginalExecutorId(task.getTaskId()));

              taskScheduledMap.getPrevTaskExecutorIdMap().remove(task.getTaskId());

              // update metadata first
              planStateManager.onTaskStateChanged(task.getTaskId(), TaskState.State.EXECUTING);

              LOG.info("{} scheduled to {} for origin", task.getTaskId(), selectedExecutor.getExecutorId());
              // send the task
              selectedExecutor.onTaskScheduled(task);
            } else {

              final Set<ExecutorRepresenter> finalCandidates = candidateExecutors.getValue()
                .stream().filter(executor ->
                  !filteredOutExecutors.contains(executor.getExecutorId())).collect(Collectors.toSet());
//                  return !(taskScheduledMap.getPrevTaskExecutorIdMap().containsKey(task.getTaskId())
//                    && taskScheduledMap.getPrevTaskExecutorIdMap()
//                    .get(task.getTaskId()).equals(executor.getExecutorId()));
//                }).collect(Collectors.toSet());

              // LOG.info("Candidate executor for {}: {}", task.getTaskId(), finalCandidates);

              if (!finalCandidates.isEmpty()) {
                // Select executor
                // For o2o-aware scheduling
                final ExecutorRepresenter selectedExecutor
                  = schedulingPolicy.selectExecutor(finalCandidates, task);

                taskScheduledMap.getPrevTaskExecutorIdMap().remove(task.getTaskId());

                // update metadata first
                planStateManager.onTaskStateChanged(task.getTaskId(), TaskState.State.EXECUTING);

                LOG.info("{} scheduled to {}", task.getTaskId(), selectedExecutor.getExecutorId());
                // send the task
                selectedExecutor.onTaskScheduled(task);
              } else {
                couldNotSchedule.add(task);
              }
            }
          });
        }
      }

      LOG.debug("All except {} were scheduled among {}", new Object[]{couldNotSchedule, taskList});
      if (couldNotSchedule.size() > 0) {
        // Try these again, if no new task list has been set
        pendingTaskCollectionPointer.addTasks(couldNotSchedule);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } else {
        return;
      }
  }


  /**
   * Signals to the condition on executor slot availability.
   */
  void onExecutorSlotAvailable() {
    schedulingIteration.signal();
  }

  /**
   * Signals to the condition on the Task collection availability.
   */
  public void onNewPendingTaskCollectionAvailable() {
    schedulingIteration.signal();
  }

  /**
   * Run the dispatcher thread.
   */
  void run() {
    if (!isTerminated && !isSchedulerRunning) {
      dispatcherThread.execute(new TaskDispatcherThread());
      dispatcherThread.shutdown();
      isSchedulerRunning = true;
    }
  }

  void terminate() {
    isTerminated = true;
    schedulingIteration.signal();
  }

  /**
   * A {@link Condition} that allows 'delayed' signaling.
   */
  private final class DelayedSignalingCondition {
    private boolean hasDelayedSignal = false;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    /**
     * Signals to this condition. If no thread is awaiting for this condition,
     * signaling is delayed until the first next {@link #await} invocation.
     */
    void signal() {
      lock.lock();
      try {
        hasDelayedSignal = true;
        condition.signal();
      } finally {
        lock.unlock();
      }
    }

    /**
     * Awaits to this condition. The thread will awake when there is a delayed signal,
     * or the next first {@link #signal} invocation.
     */
    void await() {
      lock.lock();
      try {
        if (!hasDelayedSignal) {
          condition.await();
        }
        hasDelayedSignal = false;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } finally {
        lock.unlock();
      }
    }
  }
}
