package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.TaskState;
import org.apache.nemo.common.Util;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.executor.common.tasks.TaskExecutor;
import org.apache.nemo.runtime.message.MessageSender;
import org.apache.nemo.runtime.message.PersistentConnectionToMasterMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.TASK_SCHEDULE_MAP_LISTENER_ID;

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

  private final Queue<TaskExecutor> unInitializedTasks;

  private final String executorId;

  private final Map<String, ExecutorThreadTask> taskIdExecutorMap = new ConcurrentHashMap<>();

  private final List<String> tasks;

  private ConcurrentLinkedQueue<TaskHandlingEvent> controlShortcutQueue;

  private final ControlEventHandler controlEventHandler;

  // events per sec
  private long throttleRate = 1000;

  private final boolean testing;

  private final ExecutorMetrics executorMetrics;

  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  private final MetricMessageSender metricMessageSender;

  private final MessageSender<ControlMessage.Message> taskScheduledMapSender;

  private final int index;

  private final TaskExecutorMapWrapper taskExecutorMapWrapper;

  private final TaskScheduledMapWorker taskScheduledMapWorker;

  public ExecutorThread(final int executorThreadIndex,
                        final String executorId,
                        final ControlEventHandler controlEventHandler,
                        final long throttleRate,
                        final ExecutorMetrics executorMetrics,
                        final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                        final MetricMessageSender metricMessageSender,
                        final MessageSender<ControlMessage.Message> taskScheduledMapSender,
                        final TaskExecutorMapWrapper taskExecutorMapWrapper,
                        final TaskScheduledMapWorker taskScheduledMapWorker,
                        final boolean testing) {
    this.index = executorThreadIndex;
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    this.taskScheduledMapWorker = taskScheduledMapWorker;
    this.dispatcher = Executors.newSingleThreadScheduledExecutor();
    this.executorService = Executors.newSingleThreadExecutor();
    this.throttle = new AtomicBoolean(false);
    this.queue = new ConcurrentLinkedQueue<>();
    this.controlShortcutQueue = new ConcurrentLinkedQueue<>();
    this.sourceTasks = new ArrayList<>();
    this.pendingSourceTasks = new ArrayList<>();
    this.executorId = executorId;
    this.controlEventHandler = controlEventHandler;
    this.throttleRate = 10000;
    this.testing = testing;
    this.executorMetrics = executorMetrics;
    this.unInitializedTasks = new LinkedBlockingQueue<>();
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.metricMessageSender = metricMessageSender;
    this.taskScheduledMapSender = taskScheduledMapSender;
    this.tasks = new LinkedList<>();

    final AtomicLong l = new AtomicLong(System.currentTimeMillis());

    dispatcher.scheduleAtFixedRate(() -> {
      synchronized (pendingSourceTasks) {
        synchronized (sourceTasks) {
          if (System.currentTimeMillis() - l.get() >= 2000) {
            // LOG.info("Pending source tasks: {} / active source tasks {} in executor {}", pendingSourceTasks, sourceTasks, executorId);
           // l.set(System.currentTimeMillis());
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

        synchronized (tasks) {
          tasks.remove(task.getId());
        }
        taskIdExecutorMap.remove(task.getId());
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    LOG.info("Deleting done task {} in executor {}", task.getId(), executorId);
  }

  public void addNewTask(final TaskExecutor task) {
    LOG.info("Add task {}", task.getId());
    taskIdExecutorMap.put(task.getId(), task);

    synchronized (unInitializedTasks) {
      unInitializedTasks.add(task);
    }

    synchronized (tasks) {
      tasks.add(task.getId());
    }
    LOG.info("Add task to unInitializedTasks {} / {}", task.getId(), unInitializedTasks);
  }

  public int getNumTasks() {
    synchronized (tasks) {
      return tasks.size();
    }
  }

  @Override
  public void addShortcutEvent(final TaskHandlingEvent event) {
    controlShortcutQueue.add(event);
  }

  @Override
  public void addEvent(TaskHandlingEvent event) {
    if (!event.isControlMessage()) {
      // taskExecutorMapWrapper.getTaskExecutor(event.getTaskId())
      //  .getTaskMetrics().incrementInputReceiveElement();
      executorMetrics.inputReceiveCntMap.get(this).getAndIncrement();
    }

    queue.add(event);
  }

  public void handlingControlEvent(final TaskHandlingEvent event) {
    if (Thread.currentThread().equals(currThread)) {
      controlEventHandler.handleControlEvent(event);
    } else {
      queue.add(event);
    }
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

  long currProcessedCnt = 0;
  long elapsedTime = 0L;
  long prevSleepTime = System.currentTimeMillis();
  long prevThrottleRateAdjustTime = System.currentTimeMillis();
  long backPressureReceiveTime = System.currentTimeMillis();
  long sleepTime = 0;

  private final long adjustPeriod = Util.THROTTLE_WINDOW;

  public void backpressure() {
    /*
    synchronized (tasks) {
      if (tasks.isEmpty()) {
        return;
      }
    }

    synchronized (this) {
      LOG.info("Backpressure throttle rate from {} to {}", throttleRate, Math.max(1000, (long) (throttleRate * 0.8)));
      throttleRate = Math.max(1000, (long) (throttleRate * 0.8));
      backPressureReceiveTime = System.currentTimeMillis();
      sleepTime = 0;
    }
    */
  }

  private Thread currThread;

  public Thread getCurrThread() {
    return currThread;
  }

  public void handlingDataEvent(final TaskHandlingEvent event) {
    // Handling data
    if (Thread.currentThread().equals(currThread)) {
      final String taskId = event.getTaskId();
      ExecutorThreadTask taskExecutor = taskIdExecutorMap.get(taskId);

      while (taskExecutor == null) {
        taskExecutor = taskIdExecutorMap.get(taskId);
        try {
          Thread.sleep(5);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      taskExecutor.handleData(event.getEdgeId(), event);

      /*
      final long cnt = executorMetrics.inputProcessCntMap.get(this);
      executorMetrics.inputProcessCntMap.put(this, cnt + 1);
      currProcessedCnt += 1;
      */
    } else {
      addEvent(event);
    }
  }

  private void handlingEvent(final TaskHandlingEvent event) {
    // check control message
    handlingControlEvent();

    if (event.isControlMessage()) {
      controlEventHandler.handleControlEvent(event);
    } else {
      // Handling data
      final String taskId = event.getTaskId();
      ExecutorThreadTask taskExecutor = taskIdExecutorMap.get(taskId);

      while (taskExecutor == null) {
        taskExecutor = taskIdExecutorMap.get(taskId);
        try {
          Thread.sleep(5);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      // throttling();
      taskExecutor.handleData(event.getEdgeId(), event);
      final long cnt = executorMetrics.inputProcessCntMap.get(this);
      executorMetrics.inputProcessCntMap.put(this, cnt + 1);
    }
  }

  public void start() {

    executorService.execute(() -> {
      currThread = Thread.currentThread();

      try {
        while (!finished) {

          // process source tasks
          boolean processed = false;

          if (!sourceTasks.isEmpty()) {
            synchronized (pendingSourceTasks) {
              synchronized (sourceTasks) {
                final Iterator<ExecutorThreadTask> iterator = sourceTasks.iterator();
                while (iterator.hasNext()) {
                  final ExecutorThreadTask sourceTask = iterator.next();

                  handlingControlEvent();

                  if (sourceTask.hasData()) {
                    sourceTask.handleSourceData();
                    // executorMetrics.eventProcessed.incrementAndGet();
                    processed = true;
                  } else {
                    iterator.remove();
                    pendingSourceTasks.add(sourceTask);
                    //LOG.info("Add pending task {}", sourceTask.getId());
                  }
                }
              }
            }
          }

          if (!unInitializedTasks.isEmpty()) {
            synchronized (unInitializedTasks) {
              if (!unInitializedTasks.isEmpty()) {
                unInitializedTasks.forEach(t -> {
                  final long st = System.currentTimeMillis();
                  LOG.info("Start initialization of {}", t.getId());
                  // send task schedule done message
                  final TaskStateManager taskStateManager =
                    new TaskStateManager(t.getTask(), executorId, persistentConnectionToMasterMap, metricMessageSender);

                  taskExecutorMapWrapper.putTaskExecutor(t, this);

                  //taskExecutor.execute();
                  taskStateManager.onTaskStateChanged(TaskState.State.EXECUTING, Optional.empty(), Optional.empty());

                  LOG.info("Task message send time {} to {} thread of {}, time {}", t.getId(), index, executorId,
                    System.currentTimeMillis() - st);

                  taskScheduledMapSender.send(ControlMessage.Message.newBuilder()
                    .setId(RuntimeIdManager.generateMessageId())
                    .setListenerId(TASK_SCHEDULE_MAP_LISTENER_ID.ordinal())
                    .setType(ControlMessage.MessageType.TaskExecuting)
                    .setTaskExecutingMsg(ControlMessage.TaskExecutingMessage.newBuilder()
                      .setExecutorId(executorId)
                      .setTaskId(t.getId())
                      .build())
                    .build());

                  // After putTaskExecutor and scheduling the task, we should send pipe init message
                  while (taskScheduledMapWorker
                    .getRemoteExecutorId(t.getId(), true) == null) {
                    try {
                      Thread.sleep(50);
                    } catch (InterruptedException e) {
                      e.printStackTrace();
                    }
                  }

                  LOG.info("Initializing task {}", t.getId());
                  t.initialize();
                  LOG.info("Initializing done of task {}", t.getId());

                  if (t.isSource() && !t.isOffloadedTask()) {
                    synchronized (pendingSourceTasks) {
                      pendingSourceTasks.add(t);
                    }
                  }
                });

                unInitializedTasks.clear();
              }
            }
          }

          handlingControlEvent();

          // process intermediate data
          final Iterator<TaskHandlingEvent> iterator = queue.iterator();
          while (iterator.hasNext()) {
            //LOG.info("Polling queue");
            final TaskHandlingEvent event = iterator.next();
            handlingEvent(event);
            iterator.remove();
          }

          if (!processed || (sourceTasks.isEmpty() && queue.isEmpty())) {
            Thread.sleep(1);
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

  @Override
  public String toString() {
    return "ExecutorThread" + index;
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
