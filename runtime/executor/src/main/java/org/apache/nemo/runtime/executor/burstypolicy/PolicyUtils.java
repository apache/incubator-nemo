package org.apache.nemo.runtime.executor.burstypolicy;

import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class PolicyUtils {
  private static final Logger LOG = LoggerFactory.getLogger(PolicyUtils.class.getName());

  public static StatelessTaskStatInfo measureTaskStatInfo(
    final ConcurrentMap<TaskExecutor, Boolean> taskExecutorMap) {
    int running = 0;
    int offpending = 0;
    int offloaded = 0;
    int deoffpending = 0;
    int stateless = 0;
    int stateful = 0;
    final List<TaskExecutor> runningTasks = new ArrayList<>(taskExecutorMap.size());
    final List<TaskExecutor> statelessRunningTasks = new ArrayList<>(taskExecutorMap.size());
    final List<TaskExecutor> statefulRunningTasks = new ArrayList<>(taskExecutorMap.size());
    final Map<String, TaskExecutor> taskIdTaskExecutorMap = new HashMap<>();
    for (final TaskExecutor taskExecutor : taskExecutorMap.keySet()) {
      //if (taskExecutor.isStateless()) {
      //  stateless += 1;

      taskIdTaskExecutorMap.put(taskExecutor.getId(), taskExecutor);

      /*
      if (taskExecutor.isRunning()) {
        if (taskExecutor.isStateless()) {
          stateless += 1;
          statelessRunningTasks.add(taskExecutor);
        } else {
          stateful += 1;
          statefulRunningTasks.add(taskExecutor);
        }
        runningTasks.add(taskExecutor);
        running += 1;
      } else if (taskExecutor.isOffloadPending()) {
        offpending += 1;
      } else if (taskExecutor.isOffloadedTask()) {
        offloaded += 1;
      } else if (taskExecutor.isDeoffloadPending()) {
        deoffpending += 1;
      }
      */

      // }
    }

    LOG.info("Stateless Task running {}, Stateful running {}, offload_pending: {}, offloaded: {}, deoffload_pending: {}, total: {}",
      stateless, stateful, offpending, offloaded, deoffpending, taskExecutorMap.size());

    return new StatelessTaskStatInfo(running, offpending, offloaded, deoffpending,
      stateless, runningTasks, statelessRunningTasks, statefulRunningTasks, taskIdTaskExecutorMap);
  }
}
