package org.apache.nemo.runtime.executor.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ExecutorThread {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorThread.class.getName());

  private final ConcurrentLinkedQueue<TaskExecutor> newTasks;
  private final ConcurrentLinkedQueue<TaskExecutor> deletedTasks;
  private final List<TaskExecutor> availableTasks;
  private final List<TaskExecutor> pendingTasks;

  private volatile boolean finished = false;
  //private final AtomicBoolean isPollingTime = new AtomicBoolean(false);
  private volatile boolean isPollingTime = false;
  private final ScheduledExecutorService scheduledExecutorService;
  private final ExecutorService executorService;
  private final String executorThreadName;

  private final ConcurrentMap<String, Integer> taskCounterMap = new ConcurrentHashMap<>();

  private volatile boolean closed = false;

  public ExecutorThread(final int executorThreadIndex,
                        final String executorId) {
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.newTasks = new ConcurrentLinkedQueue<>();
    this.deletedTasks = new ConcurrentLinkedQueue<>();
    this.availableTasks = new ArrayList<>();
    this.pendingTasks = new ArrayList<>();
    this.executorThreadName = executorId + "-" + executorThreadIndex;
    this.executorService = Executors.newSingleThreadExecutor();
  }

  public void deleteTask(final TaskExecutor task) {
    deletedTasks.add(task);
  }

  public void addNewTask(final TaskExecutor task) {
    newTasks.add(task);
  }

  private volatile boolean loggingTime = false;

  public void start() {

    final int batchSize = 100;

    scheduledExecutorService.scheduleAtFixedRate(() -> {
      //isPollingTime.set(true);
      isPollingTime = true;
    }, 50, 50, TimeUnit.MILLISECONDS);

    scheduledExecutorService.scheduleAtFixedRate(() -> {
      loggingTime = true;

      for (final String taskId : taskCounterMap.keySet()) {
        LOG.info("{} processed cnt: {}", taskId, taskCounterMap.remove(taskId));
      }

    }, 5, 5, TimeUnit.SECONDS);

    executorService.execute(() -> {
      try {
        while (!finished) {

          if (loggingTime) {
            loggingTime = false;
            LOG.info("{} Available tasks {}: {}, pending {}: {}",
              executorThreadName,
              availableTasks.size(), availableTasks, pendingTasks.size(), pendingTasks);
          }

          while (!newTasks.isEmpty()) {
            final TaskExecutor newTask = newTasks.poll();
            availableTasks.add(newTask);
          }

          while (!deletedTasks.isEmpty()) {
            final TaskExecutor deletedTask = deletedTasks.poll();

            LOG.info("Deleting task {}", deletedTask.getId());
            //availableTasks.remove(deletedTask);
            //pendingTasks.remove(deletedTask);

            try {
              deletedTask.close();
            } catch (Exception e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          }

          final Iterator<TaskExecutor> iterator = availableTasks.iterator();
          while (iterator.hasNext()) {
            final TaskExecutor availableTask = iterator.next();

            if (!availableTask.handleData()) {
              iterator.remove();
              pendingTasks.add(availableTask);
            }

              /*
              int processedCnt = 0;

              final long st = System.nanoTime();

              while (availableTask.handleData() && processedCnt < batchSize) {
                processedCnt += 1;
              }

              final int cnt = taskCounterMap.getOrDefault(availableTask.getId(), 0);
              taskCounterMap.put(availableTask.getId(), cnt + processedCnt);

              //LOG.info("handling task {}, cnt: {}", availableTask.getId(), processedCnt);

              final long et = System.nanoTime();
              availableTask.getTaskExecutionTime().addAndGet(et - st);

              if (processedCnt < batchSize) {
                iterator.remove();
                pendingTasks.add(availableTask);
              }
              */
          }

          if (availableTasks.isEmpty()) {
            try {
              Thread.sleep(10);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }

          if (isPollingTime) {
            isPollingTime = false;
            boolean pendingSet = false;
            // how to check whether the task is ready or not?
            for (final TaskExecutor pendingTask : pendingTasks) {
              if (!pendingTask.isFinished()) {
                availableTasks.add(pendingTask);
              } else {
                pendingTask.finish();
                pendingSet = true;
              }
            }
            pendingTasks.clear();
            if (pendingSet) {
              LOG.info("After finishign task: availables: {}, pending: {}", availableTasks, pendingTasks);
            }
          }
        }

        final List<TaskExecutor> tasks = new ArrayList<>(deletedTasks.size());

        while (!deletedTasks.isEmpty()) {
          final TaskExecutor deletedTask = deletedTasks.poll();
          tasks.add(deletedTask);

          LOG.info("Deleting task {}", deletedTask.getId());
          //availableTasks.remove(deletedTask);
          //pendingTasks.remove(deletedTask);

          try {
            deletedTask.close();
            LOG.info("Call {} close", deletedTask.getId());
          } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }

        LOG.info("deleted tasks: {}", tasks.size());

        for (final TaskExecutor deletedTask : tasks) {
          LOG.info("Finishing task {}", deletedTask.getId());
          while (!deletedTask.isFinished()) {
            Thread.sleep(100);
          }

          deletedTask.finish();
          LOG.info("Finished task {}", deletedTask.getId());
        }

        closed = true;
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
  }

  public void close() {
    finished = true;

    LOG.info("Closing executor thread...");

    while (!closed) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
