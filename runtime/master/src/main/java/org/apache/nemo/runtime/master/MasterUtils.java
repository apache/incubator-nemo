package org.apache.nemo.runtime.master;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.Task;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class MasterUtils {

  public static Pair<Map<String, Integer>, List<Task>> getMaxMigrationCntPerStage(final ExecutorRepresenter executor,
                                                                                  final List<Double> ratios,
                                                                                  final List<String> stages) {
    final Map<String, Integer> stageIdCounterMap = new HashMap<>();
    final List<Task> tasksToBeMoved = new LinkedList<>();

    executor.getScheduledTasks().stream()
      .filter(task -> stages.contains(task.getStageId()))
      .map(task -> {
        tasksToBeMoved.add(task);
        return task.getStageId();
      })
      .forEach(stageId -> {
        stageIdCounterMap.putIfAbsent(stageId, 0);
        stageIdCounterMap.put(stageId, stageIdCounterMap.get(stageId) + 1);
      });

    for (final String key : stageIdCounterMap.keySet()) {
      final int index = stages.indexOf(key);
      final double ratio = ratios.get(index);
      stageIdCounterMap.put(key, Math.min(stageIdCounterMap.get(key),
        (int) (stageIdCounterMap.get(key) * ratio)));
    }

    return Pair.of(stageIdCounterMap, tasksToBeMoved);
  }
}
