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

  private boolean reclaiming;

  @Inject
  private TaskDispatcher(final SchedulingConstraintRegistry schedulingConstraintRegistry,
                         final SchedulingPolicy schedulingPolicy,
                         final PendingTaskCollectionPointer pendingTaskCollectionPointer,
                         final ExecutorRegistry executorRegistry,
                         final PlanStateManager planStateManager,
                         final TaskScheduledMapMaster taskScheduledMap,
                         final RendevousServer rendevousServer) {
    this.pendingTaskCollectionPointer = pendingTaskCollectionPointer;
    this.dispatcherThread = Executors.newSingleThreadExecutor(runnable ->
        new Thread(runnable, "TaskDispatcher thread"));
    this.planStateManager = planStateManager;
    this.isSchedulerRunning = false;
    this.isTerminated = false;
    this.executorRegistry = executorRegistry;
    this.schedulingPolicy = schedulingPolicy;
    this.schedulingConstraintRegistry = schedulingConstraintRegistry;
    this.taskScheduledMap = taskScheduledMap;
    this.rendevousServer = rendevousServer;
    this.reclaiming = false;
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
        schedulingIteration.await();
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


  private void initialSetup() {
      // send global message
      while (!taskScheduledMap.isAllExecutorAddressReceived()) {
        LOG.info("Waiting executor address info...");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      final List<ControlMessage.LocalExecutorAddressInfoMessage> entries1 =
        taskScheduledMap.getExecutorAddressMap().entrySet()
          .stream().map(entry -> {
          return ControlMessage.LocalExecutorAddressInfoMessage.newBuilder()
            .setExecutorId(entry.getKey())
            .setAddress(entry.getValue().left())
            .setPort(entry.getValue().right())
            .build();
        }).collect(Collectors.toList());

      // send global executor address map
      executorRegistry.viewExecutors(executors -> {

        int index = 0;
        final List<String> vmAddresses = VMScalingAddresses.VM_ADDRESSES;
        final List<String> vmIds = VMScalingAddresses.INSTANCE_IDS;

        final int hop = vmAddresses.size() / executors.size();

        for (final ExecutorRepresenter executor : executors) {
          LOG.info("Send global executor address to executor {}", executor.getExecutorId());
          // TODO: send vm scaling addresses
          final int startindex = index * hop;
          final int endindex = (index + 1) == executors.size()
            ? vmAddresses.size() : (index + 1) * hop ;

          final List<String> addr = vmAddresses.subList(startindex, endindex);
          final List<String> ids = vmIds.subList(startindex, endindex);

          LOG.info("index {}, Vm addr: {}, ids: {}", index, addr, ids);

          final long id = RuntimeIdManager.generateMessageId();
          executor.sendControlMessage(ControlMessage.Message.newBuilder()
            .setId(id)
            .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
            .setType(ControlMessage.MessageType.GlobalExecutorAddressInfo)
            .setGlobalExecutorAddressInfoMsg(ControlMessage.GlobalExecutorAddressInfoMessage.newBuilder()
              .addAllInfos(entries1)
              .addAllVmAddresses(addr)
              .addAllVmIds(ids)
              .build())
            .build());

          index += 1;
        }
      });

      // send global message
      while (!taskScheduledMap.isAllRelayServerInfoReceived()) {
        LOG.info("Waiting relay server info...");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      final List<ControlMessage.LocalRelayServerInfoMessage> entries =
        taskScheduledMap.getExecutorRelayServerInfoMap().entrySet()
          .stream().map(entry -> {
          return ControlMessage.LocalRelayServerInfoMessage.newBuilder()
            .setExecutorId(entry.getKey())
            .setAddress(entry.getValue().left())
            .setPort(entry.getValue().right())
            .build();
        }).collect(Collectors.toList());

      // send global info
      executorRegistry.viewExecutors(executors -> {
        for (final ExecutorRepresenter executor : executors) {
          LOG.info("Send global relay info to executor {}", executor.getExecutorId());

          final long id = RuntimeIdManager.generateMessageId();
          executor.sendControlMessage(ControlMessage.Message.newBuilder()
            .setId(id)
            .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
            .setType(ControlMessage.MessageType.GlobalRelayServerInfo)
            .setGlobalRelayServerInfoMsg(ControlMessage.GlobalRelayServerInfoMessage.newBuilder()
              .addAllInfos(entries)
              .setRendevousAddress(rendevousServer.getPublicAddress())
              .setRendevousPort(rendevousServer.getPort())
              .build())
            .build());
        }
      });
  }

  private void doScheduleTaskList() {

    while (true) {

      final List<Task> taskList = Collections.singletonList(pendingTaskCollectionPointer.getTask());
      /* final Optional<Collection<Task>> taskListOptional = pendingTaskCollectionPointer.getAndSetNull();
      if (!taskListOptional.isPresent()) {
        // Task list is empty
        LOG.debug("PendingTaskCollectionPointer is empty. Awaiting for more Tasks...");
        return;
      }

      final Collection<Task> taskList = taskListOptional.get();
      */

      LOG.info("Size of tasks: {}", taskList.size());
      // Reverse order by stage number
      final List<List<Task>> stageTasks = getStageTasks(taskList);

      final List<Task> couldNotSchedule = new ArrayList<>();

      for (final List<Task> stageTask : stageTasks) {

        /*
        try {
          Thread.sleep(8000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        */

        for (final Task task : stageTask) {
          if (!planStateManager.getTaskState(task.getTaskId()).equals(TaskState.State.READY)) {
            // Guard against race conditions causing duplicate task launches
            LOG.debug("Skipping {} as it is not READY", task.getTaskId());
            continue;
          }

          LOG.info("Start to scheduling task {}", task);

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
              final ExecutorRepresenter selectedExecutor =
                executorRegistry.getExecutorRepresentor(
                  taskScheduledMap.getTaskOriginalExecutorId(task.getTaskId()));

              taskScheduledMap.getPrevTaskExecutorIdMap().remove(task.getTaskId());

              // update metadata first
              planStateManager.onTaskStateChanged(task.getTaskId(), TaskState.State.EXECUTING);

              LOG.info("{} scheduled to {} for origin", task.getTaskId(), selectedExecutor.getExecutorId());
              // send the task
              taskScheduledMap.addTask(task.getTaskId(), task);
              selectedExecutor.onTaskScheduled(task);

            } else {
              final Set<ExecutorRepresenter> finalCandidates = candidateExecutors.getValue()
                .stream().filter(executor -> {
                  return !(taskScheduledMap.getPrevTaskExecutorIdMap().containsKey(task.getTaskId())
                    && taskScheduledMap.getPrevTaskExecutorIdMap()
                    .get(task.getTaskId()).equals(executor.getExecutorId()));
                }).collect(Collectors.toSet());

              LOG.info("Candidate executor for {}: {}", task.getTaskId(), finalCandidates);

              if (!finalCandidates.isEmpty()) {
                // Select executor
                final ExecutorRepresenter selectedExecutor
                  = schedulingPolicy.selectExecutor(finalCandidates, task);

                taskScheduledMap.getPrevTaskExecutorIdMap().remove(task.getTaskId());

                // update metadata first
                planStateManager.onTaskStateChanged(task.getTaskId(), TaskState.State.EXECUTING);

                LOG.info("{} scheduled to {}", task.getTaskId(), selectedExecutor.getExecutorId());
                // send the task
                taskScheduledMap.addTask(task.getTaskId(), task);
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
        for (final Task t : couldNotSchedule) {
          pendingTaskCollectionPointer.addTask(t);
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } else {
        return;
      }
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
