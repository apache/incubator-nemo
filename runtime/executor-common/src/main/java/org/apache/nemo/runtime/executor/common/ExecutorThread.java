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
  private final ConcurrentLinkedQueue<TaskExecutor> availableTasks;
  private final ConcurrentLinkedQueue<TaskExecutor> pendingTasks;

  private volatile boolean finished = false;
  //private final AtomicBoolean isPollingTime = new AtomicBoolean(false);
  private volatile boolean isPollingTime = false;
  private final ScheduledExecutorService scheduledExecutorService;
  private final ExecutorService executorService;
  private final String executorThreadName;

  private final ConcurrentMap<String, Integer> taskCounterMap = new ConcurrentHashMap<>();

  private volatile boolean closed = false;

  private final AtomicBoolean throttle;

  private final ScheduledExecutorService dispatcher;

  private final ConcurrentLinkedQueue<TaskExecutor> finishedExecutors;

  public ExecutorThread(final int executorThreadIndex,
                        final String executorId) {
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.newTasks = new ConcurrentLinkedQueue<>();
    this.deletedTasks = new ConcurrentLinkedQueue<>();
    this.availableTasks = new ConcurrentLinkedQueue<>();
    this.pendingTasks = new ConcurrentLinkedQueue<>();
    this.finishedExecutors = new ConcurrentLinkedQueue<>();
    this.executorThreadName = executorId + "-" + executorThreadIndex;
    this.executorService = Executors.newSingleThreadExecutor();
    this.throttle = new AtomicBoolean(false);
    this.dispatcher = Executors.newSingleThreadScheduledExecutor();

  }

  public void deleteTask(final TaskExecutor task) {
    deletedTasks.add(task);
  }

  public void addNewTask(final TaskExecutor task) {
    newTasks.add(task);
  }

  private volatile boolean loggingTime = false;


  public AtomicBoolean getThrottle() {
    return throttle;
  }

  public void start() {

    final int batchSize = 20;

    scheduledExecutorService.scheduleAtFixedRate(() -> {
      //isPollingTime.set(true);
      isPollingTime = true;
    }, 50, 50, TimeUnit.MILLISECONDS);

    scheduledExecutorService.scheduleAtFixedRate(() -> {
      loggingTime = true;

      /*
      int totalProcessedCnt = 0;
      for (final String taskId : taskCounterMap.keySet()) {
        totalProcessedCnt += taskCounterMap.remove(taskId);
      }

      LOG.info("{} total processed cnt: {}", executorThreadName, totalProcessedCnt);
      */

    }, 5, 5, TimeUnit.SECONDS);

    dispatcher.scheduleAtFixedRate(() -> {
      final Iterator<TaskExecutor> iterator = pendingTasks.iterator();
      while (iterator.hasNext()) {
        final TaskExecutor pendingTask = iterator.next();
        if (pendingTask.hasData()) {
          iterator.remove();
          availableTasks.add(pendingTask);
        } else if (pendingTask.isFinished()) {
          iterator.remove();
          finishedExecutors.add(pendingTask);
        }
      }
    }, 50, 50, TimeUnit.MILLISECONDS);

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

          /*
          while (throttle.get()) {
            LOG.info("Throttling thread {} ...", executorThreadName);
            Thread.sleep(200);
          }
          */

          final Iterator<TaskExecutor> iterator = availableTasks.iterator();
          while (iterator.hasNext()) {
            final TaskExecutor availableTask = iterator.next();

            int batchCnt = 0;
            while (batchCnt < batchSize) {
              final int processedCnt = availableTask.handleData();
              batchCnt += 1;

              if (processedCnt == 0) {
                iterator.remove();
                pendingTasks.add(availableTask);
                break;
              }
            }

            //final int cnt = taskCounterMap.getOrDefault(availableTask.getId(), 0);
            //taskCounterMap.put(availableTask.getId(), cnt + processedCnt);

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
            Thread.sleep(10);
          }

          while (!finishedExecutors.isEmpty()) {
            final TaskExecutor finishedExecutor = finishedExecutors.poll();
            finishedExecutor.finish();
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
