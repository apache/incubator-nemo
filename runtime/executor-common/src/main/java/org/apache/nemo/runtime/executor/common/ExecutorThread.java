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
  private final ScheduledExecutorService dispatcher;
  public final ScheduledExecutorService scheduledExecutorService;
  private final ExecutorService executorService;
  private final String executorThreadName;

  private final ConcurrentMap<String, Integer> taskCounterMap = new ConcurrentHashMap<>();

  private volatile boolean closed = false;

  private final AtomicBoolean throttle;


  private final List<TaskExecutor> finishedTasks;
  private final List<TaskExecutor> finishWaitingTasks;

  public final ConcurrentLinkedQueue<Runnable> queue;

  private final List<TaskExecutor> sourceTasks;
  private final List<TaskExecutor> pendingSourceTasks;

  public final ExecutorService decoderThread = Executors.newSingleThreadExecutor();

  public ExecutorThread(final int executorThreadIndex,
                        final String executorId) {
    this.dispatcher = Executors.newSingleThreadScheduledExecutor();
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.newTasks = new ConcurrentLinkedQueue<>();
    this.deletedTasks = new ConcurrentLinkedQueue<>();
    this.finishedTasks = new ArrayList<>();
    this.finishWaitingTasks = new ArrayList<>();
    this.executorThreadName = executorId + "-" + executorThreadIndex;
    this.executorService = Executors.newSingleThreadExecutor();
    this.throttle = new AtomicBoolean(false);
    this.queue = new ConcurrentLinkedQueue<>();
    this.sourceTasks = new ArrayList<>();
    this.pendingSourceTasks = new ArrayList<>();

    dispatcher.scheduleAtFixedRate(() -> {
      synchronized (pendingSourceTasks) {
        //LOG.info("Pending source tasks: {}", pendingSourceTasks.size());
        final Iterator<TaskExecutor> iterator = pendingSourceTasks.iterator();
        while (iterator.hasNext()) {
          final TaskExecutor sourceTask = iterator.next();
          if (sourceTask.isSourceAvailable()) {
            iterator.remove();
            synchronized (sourceTasks) {
              //LOG.info("Add available source: {}", sourceTask.getId());
              sourceTasks.add(sourceTask);
            }
          }
        }
      }
    }, 20, 20, TimeUnit.MILLISECONDS);
  }

  public void deleteTask(final TaskExecutor task) {
    deletedTasks.add(task);
  }

  public void addNewTask(final TaskExecutor task) {
    LOG.info("Add task {}", task.getId());
    if (task.isSource()) {
      synchronized (pendingSourceTasks) {
        pendingSourceTasks.add(task);
      }
    }
  }

  private volatile boolean loggingTime = false;

  public AtomicBoolean getThrottle() {
    return throttle;
  }

  private void checkDeleteTasks() {
    while (!deletedTasks.isEmpty()) {
      final TaskExecutor deletedTask = deletedTasks.poll();

      synchronized (pendingSourceTasks) {
        pendingSourceTasks.remove(deletedTask);
        synchronized (sourceTasks) {
          sourceTasks.remove(deletedTask);
        }
      }

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

    if (!finishedTasks.isEmpty()) {
      final Iterator<TaskExecutor> iterator = finishedTasks.iterator();
      while (iterator.hasNext()) {
        final TaskExecutor finishedExecutor = iterator.next();
        if (finishedExecutor.isFinished()) {
          finishedExecutor.finish();
          finishWaitingTasks.add(finishedExecutor);
          iterator.remove();
        }
      }
    }

    if (!finishWaitingTasks.isEmpty()) {
      finishWaitingTasks.removeIf(executor -> executor.isFinishDone());
    }
  }

  public void start() {

    executorService.execute(() -> {
      try {
        while (!finished) {

          checkDeleteTasks();

          // process source tasks
          final List<TaskExecutor> pendings = new ArrayList<>();

          boolean processed = false;

          if (!throttle.get()) {
            synchronized (sourceTasks) {
              final Iterator<TaskExecutor> iterator = sourceTasks.iterator();
              while (iterator.hasNext()) {
                final TaskExecutor sourceTask = iterator.next();
                if (sourceTask.isSourceAvailable()) {
                  sourceTask.handleSourceData();
                } else {
                  iterator.remove();
                  //LOG.info("Add pending task {}", sourceTask.getId());
                  pendings.add(sourceTask);
                }
              }
            }
          }

          synchronized (pendingSourceTasks) {
            pendingSourceTasks.addAll(pendings);
          }

          // process intermediate data
          final Iterator<Runnable> runnableIterator = queue.iterator();
          while (runnableIterator.hasNext()) {
            final Runnable runnable = runnableIterator.next();
            runnableIterator.remove();
            //LOG.info("Polling queue");
            runnable.run();
            processed = true;
          }

          if (throttle.get() && !processed) {
            Thread.sleep(20);
          }

          if (sourceTasks.isEmpty() && queue.isEmpty()) {
            Thread.sleep(20);
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
          finishWaitingTasks.add(deletedTask);
          LOG.info("Finished task {}", deletedTask.getId());
        }


        while (!finishWaitingTasks.isEmpty()) {
          finishWaitingTasks.removeIf(executor -> executor.isFinishDone());
          Thread.sleep(100);
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
