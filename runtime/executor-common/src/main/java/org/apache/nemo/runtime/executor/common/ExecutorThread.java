package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Pair;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class ExecutorThread implements ExecutorThreadQueue {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorThread.class.getName());

  private volatile boolean finished = false;
  //private final AtomicBoolean isPollingTime = new AtomicBoolean(false);
  private final ScheduledExecutorService dispatcher;
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

  private final long throttleRate;

  private final boolean testing;

  private final ExecutorMetrics executorMetrics;

  public ExecutorThread(final int executorThreadIndex,
                        final String executorId,
                        final ControlEventHandler controlEventHandler,
                        final long throttleRate,
                        final ExecutorMetrics executorMetrics,
                        final boolean testing) {
    this.dispatcher = Executors.newSingleThreadScheduledExecutor();
    this.executorService = Executors.newSingleThreadExecutor();
    this.throttle = new AtomicBoolean(false);
    this.queue = new ConcurrentLinkedQueue<>();
    this.controlShortcutQueue = new ConcurrentLinkedQueue<>();
    this.sourceTasks = new ArrayList<>();
    this.pendingSourceTasks = new ArrayList<>();
    this.executorId = executorId;
    this.controlEventHandler = controlEventHandler;
    this.throttleRate = throttleRate;
    this.testing = testing;
    this.executorMetrics = executorMetrics;

    final AtomicLong l = new AtomicLong(System.currentTimeMillis());

    dispatcher.scheduleAtFixedRate(() -> {
      synchronized (pendingSourceTasks) {
        synchronized (sourceTasks) {
          if (System.currentTimeMillis() - l.get() >= 2000) {
            LOG.info("Pending source tasks: {} / active source tasks {} in executor {}", pendingSourceTasks, sourceTasks, executorId);
            l.set(System.currentTimeMillis());
          }
          final Iterator<ExecutorThreadTask> iterator = pendingSourceTasks.iterator();
          while (iterator.hasNext()) {
            final ExecutorThreadTask sourceTask = iterator.next();
            if (sourceTask.isSourceAvailable()) {
              iterator.remove();
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
    executorMetrics.taskInputProcessRateMap
      .put(task.getId(), Pair.of(new AtomicLong(), new AtomicLong()));

    if (task.isSource() && !task.isOffloadedTask()) {
      synchronized (pendingSourceTasks) {
        pendingSourceTasks.add(task);
      }
    }
  }

  @Override
  public void addShortcutEvent(final TaskHandlingEvent event) {
    controlShortcutQueue.add(event);
  }

  @Override
  public void addEvent(TaskHandlingEvent event) {
    if (!event.isControlMessage() && !event.isOffloadingMessage()) {
      executorMetrics.taskInputProcessRateMap
        .get(event.getTaskId()).left().incrementAndGet();
    }

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
      if (event.isOffloadingMessage()) {
        final String taskId = event.getTaskId();
        final ExecutorThreadTask taskExecutor = taskIdExecutorMap.get(taskId);
        taskExecutor.handleData(event.getEdgeId(), event);
      } else {
        controlEventHandler.handleControlEvent(event);
      }
      controlIterator.remove();
    }
  }

  long currProcessedCnt = 0;
  long elapsedTime = 0L;

  private void throttling() {
    if (testing) {
      // throttling
      // nano to sec
      final long elapsedTimeMs = TimeUnit.NANOSECONDS.toMillis(elapsedTime);
      if (elapsedTimeMs >= 1) {
        final long desiredElapsedTime = (long) (currProcessedCnt * 1000 / throttleRate);
        if (desiredElapsedTime > elapsedTimeMs) {
          LOG.info("Throttling.. current processed cnt: {}/elapsed: {} ms, throttleRate: {}, sleep {} ms",
            currProcessedCnt, elapsedTimeMs, throttleRate, desiredElapsedTime - elapsedTimeMs);
          try {
            Thread.sleep(desiredElapsedTime - elapsedTimeMs);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        elapsedTime = 0;
        currProcessedCnt = 0;
      }
    }
  }

  public void start() {

    executorService.execute(() -> {
      try {
        while (!finished) {

          // process source tasks
          boolean processed = false;

          synchronized (pendingSourceTasks) {
            synchronized (sourceTasks) {
              final Iterator<ExecutorThreadTask> iterator = sourceTasks.iterator();
              while (iterator.hasNext()) {
                final ExecutorThreadTask sourceTask = iterator.next();

                throttling();

                if (sourceTask.hasData()) {
                  if (testing) {
                    currProcessedCnt += 1;
                    long st = System.nanoTime();
                    sourceTask.handleSourceData();
                    // executorMetrics.eventProcessed.incrementAndGet();
                    long et = System.nanoTime();
                    elapsedTime += (et - st);
                  } else {
                    sourceTask.handleSourceData();
                    // executorMetrics.eventProcessed.incrementAndGet();
                  }
                  processed = true;
                } else {
                  iterator.remove();
                  pendingSourceTasks.add(sourceTask);
                  //LOG.info("Add pending task {}", sourceTask.getId());
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

              throttling();

              if (testing) {
                long st = System.nanoTime();
                taskExecutor.handleData(event.getEdgeId(), event);
                executorMetrics.taskInputProcessRateMap
                  .get(event.getTaskId()).right().incrementAndGet();
                long et = System.nanoTime();
                elapsedTime += (et - st);
              } else {
                taskExecutor.handleData(event.getEdgeId(), event);
                executorMetrics.taskInputProcessRateMap
                  .get(event.getTaskId()).right().incrementAndGet();
              }

              processed = true;
              currProcessedCnt += 1;
            }

            iterator.remove();
          }

          if (!processed || (sourceTasks.isEmpty() && queue.isEmpty())) {
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

    /*
    while (!queue.isEmpty()) {
      LOG.info("Waiting for executor {}, numEvent: {}",  executorId, queue);
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    */

    while (!closed) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
