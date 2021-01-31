package org.apache.nemo.runtime.executor.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class ExecutorThread implements ExecutorThreadQueue {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorThread.class.getName());

  private volatile boolean finished = false;
  //private final AtomicBoolean isPollingTime = new AtomicBoolean(false);
  private final ScheduledExecutorService dispatcher;
  public final ScheduledExecutorService scheduledExecutorService;
  private final ExecutorService executorService;

  private volatile boolean closed = false;

  private final AtomicBoolean throttle;

  // <taskId, serializer, bytebuf>
  private final ConcurrentLinkedQueue<TaskHandlingEvent> queue;

  private final List<ExecutorThreadTask> sourceTasks;
  private final List<ExecutorThreadTask> pendingSourceTasks;

  private final String executorId;

  private final Map<String, ExecutorThreadTask> taskIdExecutorMap = new ConcurrentHashMap<>();

  private ConcurrentLinkedQueue<TaskHandlingEvent> controlShortcutQueue;

  private final ControlEventHandler controlEventHandler;

  public ExecutorThread(final int executorThreadIndex,
                        final String executorId,
                        final ControlEventHandler controlEventHandler) {
    this.dispatcher = Executors.newSingleThreadScheduledExecutor();
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.executorService = Executors.newSingleThreadExecutor();
    this.throttle = new AtomicBoolean(false);
    this.queue = new ConcurrentLinkedQueue<>();
    this.controlShortcutQueue = new ConcurrentLinkedQueue<>();
    this.sourceTasks = new ArrayList<>();
    this.pendingSourceTasks = new ArrayList<>();
    this.executorId = executorId;
    this.controlEventHandler = controlEventHandler;

    final AtomicLong l = new AtomicLong(System.currentTimeMillis());

    dispatcher.scheduleAtFixedRate(() -> {
      synchronized (pendingSourceTasks) {
        if (System.currentTimeMillis() - l.get() >= 1000) {
          LOG.info("Pending source tasks: {} / active source tasks {} in executor {}", pendingSourceTasks, sourceTasks, executorId);
          l.set(System.currentTimeMillis());
        }
        final Iterator<ExecutorThreadTask> iterator = pendingSourceTasks.iterator();
        while (iterator.hasNext()) {
          final ExecutorThreadTask sourceTask = iterator.next();
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

  public void deleteTask(final ExecutorThreadTask task) {
    LOG.info("Deleting task {} in executor {}", task.getId(), executorId);
    try {
      if (task.isSource()) {
        synchronized (pendingSourceTasks) {
          synchronized (sourceTasks) {
            pendingSourceTasks.remove(task);
            sourceTasks.remove(task);
          }
        }

        taskIdExecutorMap.remove(task.getId());
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    LOG.info("Deleting done task {} in executor {}", task.getId(), executorId);
  }

  public void addNewTask(final ExecutorThreadTask task) {
    LOG.info("Add task {}", task.getId());
    taskIdExecutorMap.put(task.getId(), task);

    if (task.isSource()) {
      synchronized (pendingSourceTasks) {
        pendingSourceTasks.add(task);
      }
    }
  }

  public void addShortcutEvent(final TaskHandlingEvent event) {
    controlShortcutQueue.add(event);
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

  private void handlingControlEvent() {
    final Iterator<TaskHandlingEvent> controlIterator = controlShortcutQueue.iterator();
    while (controlIterator.hasNext()) {
      // Handling control event
      final TaskHandlingEvent event = controlIterator.next();
      controlEventHandler.handleControlEvent(event);
      controlIterator.remove();
    }
  }

  public void start() {

    executorService.execute(() -> {
      try {
        while (!finished) {

          // process source tasks
          boolean processed = false;

          if (!throttle.get()) {
            synchronized (pendingSourceTasks) {
              synchronized (sourceTasks) {
                final Iterator<ExecutorThreadTask> iterator = sourceTasks.iterator();
                while (iterator.hasNext()) {
                  final ExecutorThreadTask sourceTask = iterator.next();
                  if (sourceTask.hasData()) {
                    sourceTask.handleSourceData();
                  } else {
                    iterator.remove();
                    pendingSourceTasks.add(sourceTask);
                    //LOG.info("Add pending task {}", sourceTask.getId());
                  }
                }
              }
            }
          }

          handlingControlEvent();

          // process intermediate data
          final Iterator<TaskHandlingEvent> iterator = queue.iterator();
          while (iterator.hasNext()) {
            // check control message
            handlingControlEvent();

            final TaskHandlingEvent event = iterator.next();
            //LOG.info("Polling queue");
            if (event.isControlMessage()) {
              controlEventHandler.handleControlEvent(event);
            } else {
              // Handling data
              final String taskId = event.getTaskId();
              final ExecutorThreadTask taskExecutor = taskIdExecutorMap.get(taskId);
              taskExecutor.handleData(event.getDataFetcher(), event);
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
