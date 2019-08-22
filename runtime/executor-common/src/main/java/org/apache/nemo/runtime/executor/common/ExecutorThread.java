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

  private volatile boolean finished = false;
  //private final AtomicBoolean isPollingTime = new AtomicBoolean(false);
  private volatile boolean isPollingTime = false;
  public final ScheduledExecutorService scheduledExecutorService;
  private final ExecutorService executorService;
  private final String executorThreadName;

  private final ConcurrentMap<String, Integer> taskCounterMap = new ConcurrentHashMap<>();

  private volatile boolean closed = false;

  private final AtomicBoolean throttle;


  private final List<TaskExecutor> finishedTasks;

  public final ConcurrentLinkedQueue<Runnable> queue;

  private List<TaskExecutor> tasks;

  public ExecutorThread(final int executorThreadIndex,
                        final String executorId) {
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.newTasks = new ConcurrentLinkedQueue<>();
    this.deletedTasks = new ConcurrentLinkedQueue<>();
    this.finishedTasks = new ArrayList<>();
    this.executorThreadName = executorId + "-" + executorThreadIndex;
    this.executorService = Executors.newSingleThreadExecutor();
    this.throttle = new AtomicBoolean(false);
    this.queue = new ConcurrentLinkedQueue<>();
    this.tasks = new ArrayList<>();
  }

  public void deleteTask(final TaskExecutor task) {
    deletedTasks.add(task);
  }

  public void addNewTask(final TaskExecutor task) {
    synchronized (tasks) {
      tasks.add(task);
    }
    //newTasks.add(task);
  }

  private volatile boolean loggingTime = false;


  public AtomicBoolean getThrottle() {
    return throttle;
  }

  public void start() {

    executorService.execute(() -> {
      try {
        while (!finished) {

          while (!deletedTasks.isEmpty()) {
            final TaskExecutor deletedTask = deletedTasks.poll();
            tasks.remove(deletedTask);

            LOG.info("Deleting task {}", deletedTask.getId());
            //availableTasks.remove(deletedTask);
            //pendingTasks.remove(deletedTask);

            try {
              deletedTask.close();
              finishedTasks.add(deletedTask);
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

          // for source
          boolean processed = false;
          synchronized (tasks) {
            for (final TaskExecutor taskExecutor : tasks) {
              processed = processed || taskExecutor.handleSourceData();
            }
          }

          // four intermediate data
          while (!queue.isEmpty()) {
            final Runnable runnable = queue.poll();
            runnable.run();

            while (!deletedTasks.isEmpty()) {
              final TaskExecutor deletedTask = deletedTasks.poll();
              tasks.remove(deletedTask);

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

            if (!finishedTasks.isEmpty()) {
              final Iterator<TaskExecutor> iterator = finishedTasks.iterator();
              while (iterator.hasNext()) {
                final TaskExecutor finishedExecutor = iterator.next();
                if (finishedExecutor.isFinished()) {
                  finishedExecutor.finish();
                  iterator.remove();
                }
              }
            }
          }

          if (!processed && queue.isEmpty()) {
            Thread.sleep(10);
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
