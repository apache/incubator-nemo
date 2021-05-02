package org.apache.nemo.runtime.master;

import org.apache.nemo.common.Task;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.apache.nemo.runtime.master.scheduler.PairStageTaskManager;
import org.apache.nemo.runtime.master.scheduler.TaskDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public final class ScaleInOutManager {
  private static final Logger LOG = LoggerFactory.getLogger(ScaleInOutManager.class.getName());

  private final TaskScheduledMapMaster taskScheduledMapMaster;
  private final TaskDispatcher taskDispatcher;
  private final ExecutorRegistry executorRegistry;
  private final PairStageTaskManager pairStageTaskManager;



  @Inject
  private ScaleInOutManager(final TaskDispatcher taskDispatcher,
                            final ExecutorRegistry executorRegistry,
                            final PairStageTaskManager pairStageTaskManager,
                            final TaskScheduledMapMaster taskScheduledMapMaster) {
    this.taskDispatcher = taskDispatcher;
    this.executorRegistry = executorRegistry;
    this.taskScheduledMapMaster = taskScheduledMapMaster;
    this.pairStageTaskManager = pairStageTaskManager;
  }

  public synchronized List<Future<String>> sendMigration(final double ratio,
                                                 final Collection<ExecutorRepresenter> executors,
                                                 final Collection<String> stages,
                                                 final boolean lambdaAffinity) {

    // Set filtered out executors to task dispatcher
    taskDispatcher.setFilteredOutExecutors(executors.stream()
      .map(e -> e.getExecutorId()).collect(Collectors.toSet()));

    final List<Future<String>> futures = new LinkedList<>();

    executors.stream().forEach(executor -> {
      // find list of tasks that the lambda executor has
      // executor 마다 정해진 number의 task들을 옮김.
      final Map<String, Integer> stageIdCounterMap = new HashMap<>();
      final Map<String, Integer> stageIdMoveCounterMap = new HashMap<>();
      final List<Task> tasksToBeMoved = new LinkedList<>();

      executor.getRunningTasks().stream()
        .filter(task -> stages.contains(task.getStageId()))
        .map(task -> {
          checkTaskMoveValidation(task, executor);
          tasksToBeMoved.add(task);
          return task.getStageId();
        })
        .forEach(stageId -> {
          stageIdCounterMap.putIfAbsent(stageId, 0);
          stageIdCounterMap.put(stageId, stageIdCounterMap.get(stageId) + 1);
        });

      for (final String key : stageIdCounterMap.keySet()) {
        stageIdCounterMap.put(key, Math.min(stageIdCounterMap.get(key),
          (int) (stageIdCounterMap.get(key) * ratio + 1)));
      }

      LOG.info("Number of tasks to move in {}: stages {}, {}", executor.getExecutorId(),
        stages, stageIdCounterMap);

      tasksToBeMoved.stream()
        .forEach(task -> {
          // check validation
          final int maxCnt = stageIdCounterMap.get(task.getStageId());
          if (stageIdMoveCounterMap.getOrDefault(task.getStageId(), 0) < maxCnt) {

            if ((task.isParitalCombine() && task.isTransientTask())
              && executor.getContainerType().equals(ResourcePriorityProperty.LAMBDA)) {
              // Deactivation task if possible
              LOG.info("Deactivate lambda task {} in {}", task.getTaskId(), executor.getExecutorId());
              futures.add(taskScheduledMapMaster.deactivateAndStopTask(task.getTaskId(), false));
            } else {
              LOG.info("Stop task {} from {}", task.getTaskId(), executor.getExecutorId());
              futures.add(taskScheduledMapMaster.stopTask(task.getTaskId(), lambdaAffinity));
            }

            stageIdMoveCounterMap.putIfAbsent(task.getStageId(), 0);
            stageIdMoveCounterMap.put(task.getStageId(), stageIdMoveCounterMap.get(task.getStageId()) + 1);
          }
        });
    });

    return futures;
  }

  private void checkTaskMoveValidation(final Task task, final ExecutorRepresenter ep) {
    if (task.isParitalCombine() && task.isVMTask())  {
      throw new RuntimeException("Cannot move task " + task.getTaskId() + " from " + ep.getExecutorId());
    } else if (task.isCrTask() || task.isStreamTask()) {
      throw new RuntimeException("Cannot move task " + task.getTaskId() + " from " + ep.getExecutorId());
    }
//
//    else if (!(task.isParitalCombine() || task.getUpstreamTaskSet().size() > 1)) {
//      throw new RuntimeException("Cannot move task " + task.getTaskId() + " from " + ep.getExecutorId());
//    }
  }

  public synchronized List<Future<String>> sendMigrationAllStages(
    final double ratio,
    final Collection<ExecutorRepresenter> executors,
    final boolean lambdaAffinity) {

    final Set<String> mergerTasks = executors.stream()
      .map(executor -> executor.getRunningTasks())
      .flatMap(l -> l.stream()
        .filter(task -> task.isMerger())
        .map(t -> t.getTaskId()))
      .collect(Collectors.toSet());

    // For each executor, move ratio * num tasks of each stage;
    final Set<String> stages =  executors.stream()
      .map(vmExecutor -> vmExecutor.getRunningTasks())
      .flatMap(l -> l.stream()
        .filter(task -> !task.isCrTask())
        .filter(task -> !task.isStreamTask())
        .filter(task -> !(task.isParitalCombine() && task.isVMTask()))
        .filter(task -> {
          // Filter out stateless task in merger->stateless task
          // Because we will grouping them and stop together in R3
          final Set<String> intersection = new HashSet<>(mergerTasks);
          intersection.retainAll(task.getUpstreamTaskSet());
          return intersection.isEmpty();
        })
        .map(t -> t.getStageId()))
      .collect(Collectors.toSet());

    return sendMigration(ratio, executors, stages, lambdaAffinity);
  }
}
