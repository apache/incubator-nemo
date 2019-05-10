package org.apache.nemo.runtime.executor.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ExecutorThread {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorThread.class.getName());

  private final ConcurrentLinkedQueue<TaskExecutor> newTasks;
  private final ConcurrentLinkedQueue<TaskExecutor> deletedTasks;
  private final List<TaskExecutor> availableTasks;
  private final List<TaskExecutor> pendingTasks;

  private volatile boolean finished = false;
  private final AtomicBoolean isPollingTime = new AtomicBoolean(false);
  private final ScheduledExecutorService scheduledExecutorService;
  private final ExecutorService executorService;
  private final String executorThreadName;

  public ExecutorThread(final ScheduledExecutorService scheduledExecutorService,
                        final int executorThreadIndex,
                        final String executorId) {
    this.scheduledExecutorService = scheduledExecutorService;
    this.newTasks = new ConcurrentLinkedQueue<>();
    this.deletedTasks = new ConcurrentLinkedQueue<>();
    this.availableTasks = new LinkedList<>();
    this.pendingTasks = new LinkedList<>();
    this.executorThreadName = executorId + "-" + executorThreadIndex;
    this.executorService = Executors.newSingleThreadExecutor();
  }

  public void deleteTask(final TaskExecutor task) {
    deletedTasks.add(task);
  }

  public void addNewTask(final TaskExecutor task) {
    newTasks.add(task);
  }

  public void start() {

    final int batchSize = 100;

    scheduledExecutorService.scheduleAtFixedRate(() -> {
      isPollingTime.set(true);
    }, 500, 500, TimeUnit.MILLISECONDS);

    executorService.execute(() -> {
      while (!finished) {

        while (!newTasks.isEmpty()) {
          final TaskExecutor newTask = newTasks.poll();
          availableTasks.add(newTask);
        }

        while (!deletedTasks.isEmpty()) {
          final TaskExecutor deletedTask = newTasks.poll();

          LOG.info("Deleting task {}", deletedTask.getId());
          availableTasks.remove(deletedTask);
          pendingTasks.remove(deletedTask);

          try {
            deletedTask.close();
          } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }

        while (!availableTasks.isEmpty()) {
          final Iterator<TaskExecutor> iterator = availableTasks.iterator();
          while (iterator.hasNext()) {
            final TaskExecutor availableTask = iterator.next();
            int processedCnt = 0;

            final long st = System.nanoTime();

            while (availableTask.handleData() && processedCnt < batchSize) {
              processedCnt += 1;
            }
            iterator.remove();

            final long et = System.nanoTime();
            availableTask.getTaskExecutionTime().addAndGet(et - st);

            if (processedCnt < batchSize) {
              pendingTasks.add(availableTask);
            }

          }
        }

        try {
          Thread.sleep(300);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        if (isPollingTime.get()) {
          isPollingTime.set(false);
          // how to check whether the task is ready or not?
          availableTasks.addAll(pendingTasks);
          pendingTasks.clear();
        }
      }
    });
  }

  public void close() {
    finished = true;
  }
}
