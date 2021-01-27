package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.offloading.common.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class ExecutorThread implements ExecutorThreadQueue {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorThread.class.getName());

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

  // <taskId, serializer, bytebuf>
  private final ConcurrentLinkedQueue<TaskHandlingEvent> queue;

  private final List<TaskExecutor> sourceTasks;
  private final List<TaskExecutor> pendingSourceTasks;

  public final ExecutorService decoderThread = Executors.newSingleThreadExecutor();

  private final String executorId;

  private final EventHandler<String> taskDoneHandler;

  private final Map<String, TaskExecutor> taskIdExecutorMap = new ConcurrentHashMap<>();

  public ExecutorThread(final int executorThreadIndex,
                        final String executorId,
                        final EventHandler<String> taskDoneHandler) {
    this.dispatcher = Executors.newSingleThreadScheduledExecutor();
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.deletedTasks = new ConcurrentLinkedQueue<>();
    this.finishedTasks = new ArrayList<>();
    this.finishWaitingTasks = new ArrayList<>();
    this.executorThreadName = executorId + "-" + executorThreadIndex;
    this.executorService = Executors.newSingleThreadExecutor();
    this.throttle = new AtomicBoolean(false);
    this.queue = new ConcurrentLinkedQueue<>();
    this.sourceTasks = new ArrayList<>();
    this.pendingSourceTasks = new ArrayList<>();
    this.executorId = executorId;
    this.taskDoneHandler = taskDoneHandler;

    final AtomicLong l = new AtomicLong(System.currentTimeMillis());

    dispatcher.scheduleAtFixedRate(() -> {
      synchronized (pendingSourceTasks) {
        if (System.currentTimeMillis() - l.get() >= 1000) {
          LOG.info("Pending source tasks: {}", pendingSourceTasks.size());
          l.set(System.currentTimeMillis());
        }
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
    taskIdExecutorMap.put(task.getId(), task);

    if (task.isSource()) {
      synchronized (pendingSourceTasks) {
        pendingSourceTasks.add(task);
      }
    }
  }

  @Override
  public void addEvent(TaskHandlingEvent event) {
    queue.add(event);
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
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
      finishWaitingTasks.removeIf(executor -> {
        if (executor.isFinishDone()) {
          // Task stop handler
          taskDoneHandler.onNext(executor.getId());
          return true;
        } else {
          return false;
        }
      });
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
          final Iterator<TaskHandlingEvent> iterator = queue.iterator();
          while (iterator.hasNext()) {
            final TaskHandlingEvent event = iterator.next();
            //LOG.info("Polling queue");

            if (event.isControlMessage()) {
              // TODO
            } else {
              final String taskId = event.getTaskId();
              final Object data = event.getData();
              final TaskExecutor taskExecutor = taskIdExecutorMap.get(taskId);
              taskExecutor.handleData(event.getDataFetcher(), data);
              processed = true;
            }

            iterator.remove();
          }

          if (throttle.get() && !processed ||
            (sourceTasks.isEmpty() && queue.isEmpty())) {
            Thread.sleep(5);
          }
        }
        // Done event while loop


        // After Finished !!
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
            Thread.sleep(10);
          }

          deletedTask.finish();
          finishWaitingTasks.add(deletedTask);
          LOG.info("Finished task {}", deletedTask.getId());
        }


        while (!finishWaitingTasks.isEmpty()) {
          finishWaitingTasks.removeIf(executor -> {
            if (executor.isFinishDone()) {
              // Task stop handler
              taskDoneHandler.onNext(executor.getId());
              return true;
            } else {
              return false;
            }
          });
          Thread.sleep(10);
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

    while (!queue.isEmpty()) {
      LOG.info("Waiting for executor {}, numEvent: {}",  executorId, queue);
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    while (!closed) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
