package org.apache.nemo.runtime.executor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.client.StreamingLambdaWorkerProxy;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.lambdaexecutor.*;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingExecutorSerializer;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingTask;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This should be called by one thread.
 * @param <I>
 * @param <O>
 */
public final class TinyTaskOffloadingWorkerManager<I, O> implements ServerlessExecutorService<I, O> {
  private static final Logger LOG = LoggerFactory.getLogger(TinyTaskOffloadingWorkerManager.class.getName());
  private final OffloadingWorkerFactory workerFactory;

  private final List<Pair<Long, TinyTaskWorker>> workers;

  // buffer that contains bytes for initializing workers


  private final ConcurrentMap<String, EventHandler<Object>> eventHandlerMap;

  private boolean finished = false;
  private volatile boolean shutdown = false;

  final AtomicLong st = new AtomicLong(System.currentTimeMillis());

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  private final ConcurrentMap<String, TaskExecutor> offloadedTaskMap = new ConcurrentHashMap<>();
  private final OffloadingTransform offloadingTransform;

  private final ConcurrentMap<String, TinyTaskWorker> deletePendingWorkers = new ConcurrentHashMap<>();

  private final EvalConf evalConf;

  private final OffloadingSerializer offloadingSerializer = new OffloadingExecutorSerializer();


  private final ByteBuf workerInitBuffer = PooledByteBufAllocator.DEFAULT.buffer();

  private final AtomicBoolean offloadingTransformSerialized = new AtomicBoolean(false);

  private final SFTaskMetrics sfTaskMetrics;

  public TinyTaskOffloadingWorkerManager(
    final OffloadingWorkerFactory workerFactory,
    final OffloadingTransform offloadingTransform,
    final EvalConf evalConf,
    final SFTaskMetrics sfTaskMetrics) {

    this.evalConf = evalConf;

    this.sfTaskMetrics = sfTaskMetrics;

    LOG.info("Start cached pool serverless executor service");

    this.offloadingTransform = offloadingTransform;
    this.eventHandlerMap = new ConcurrentHashMap<>();

    this.workerFactory = workerFactory;
    this.workers = new LinkedList<>();


    scheduler.scheduleAtFixedRate(() -> {
      schedulingWorkers();
    }, 2, 2, TimeUnit.SECONDS);
  }

  private void schedulingWorkers() {
     try {
       synchronized (workers) {
         final Iterator<Pair<Long, TinyTaskWorker>> iterator = workers.iterator();
         while (iterator.hasNext()) {
           final Pair<Long, TinyTaskWorker> pair = iterator.next();
           final TinyTaskWorker taskWorker = pair.right();

           LOG.info("Worker {}, scheduled: {}, pending: {}, workers size: {}",
             taskWorker, taskWorker.getNumScheduledTasks(), taskWorker.getNumPendingTasks(), workers.size());

           if (taskWorker.isReady()) {
             taskWorker.executePending();
           }
         }
       }
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
  }

  private StreamingLambdaWorkerProxy createNewWorker() {


    // create new worker
    LOG.info("Creating new worker... current num: {}", workers.size());

    final StreamingLambdaWorkerProxy worker = (StreamingLambdaWorkerProxy)
      workerFactory.createStreamingWorker(workerInitBuffer.retain(), offloadingSerializer, (event) -> {
        // TODO: We should retrieve states (checkpointmark, operator states, and so on)

        final Pair<String, Object> pair = (Pair<String, Object>) event;
        final Object msg = pair.right();
        //LOG.info("Receive data for {} / {}", pair.left(), pair.right());

        TaskExecutor te = offloadedTaskMap.get(pair.left());

        if (te == null) {
          LOG.info("TaskExecutor is null: {}, {}, {}", pair.left(), pair.right(), offloadedTaskMap);
          throw new RuntimeException("null");
        }

        if (msg instanceof OffloadingHeartbeatEvent) {
          final OffloadingHeartbeatEvent heartbeatEvent = (OffloadingHeartbeatEvent) msg;
          //LOG.info("Set task metrics from sf", heartbeatEvent.taskMetrics);

          for (final Pair<String, TaskMetrics.RetrievedMetrics> metric : heartbeatEvent.taskMetrics) {
            sfTaskMetrics.sfTaskMetrics.put(metric.left(), metric.right());

          }

          sfTaskMetrics.cpuLoadMap.put(heartbeatEvent.executorId, heartbeatEvent.cpuUse);

        } else if (msg instanceof OffloadingResultEvent) {
          if (((OffloadingResultEvent) msg).data.size() > 0) {
            //LOG.info("Result received: cnt {}", ((OffloadingResultEvent) msg).data.size());
            te.handleOffloadingEvent(msg);
          }
        } else if (msg instanceof ThpEvent) {
          // just print the log
          final ThpEvent thpEvent = (ThpEvent) msg;
          LOG.info("Thp: {} at {}/{} from lambda", thpEvent.thp, thpEvent.opId, thpEvent.taskId);
        } else if (msg instanceof KafkaOffloadingOutput || msg instanceof StateOutput) {
          // End of the task!

          final TinyTaskWorker taskWorker = deletePendingWorkers.remove(te.getId());
          LOG.info("Pending Output: {}, delePendingWorker: {}, hasNoTask: {}, deletePending: {}, pendingWorkers: {}",
            te.getId(), taskWorker, taskWorker.hasNoTask(), taskWorker.getDeletePending(), deletePendingWorkers);

          final int pendingCnt = taskWorker.getDeletePending().decrementAndGet();

          if (taskWorker.hasNoTask() && pendingCnt == 0) {
            taskWorker.close();
            removeRunningWorker(taskWorker);
          }

          te.handleOffloadingEvent(msg);
        } else {

          te.handleOffloadingEvent(msg);
        }
      });

    return worker;
  }

  private void removeRunningWorker(final TinyTaskWorker worker) {
    LOG.info("Remove worker {} from workers", worker);
    synchronized (workers) {
      final Iterator<Pair<Long, TinyTaskWorker>> iterator = workers.iterator();
      while (iterator.hasNext()) {
        final Pair<Long, TinyTaskWorker> pair = iterator.next();
        if (pair.right().equals(worker)) {
          iterator.remove();
          break;
        }
      }
    }
  }

  private TinyTaskWorker findExecutableWorker() {
    synchronized (workers) {
      for (final Pair<Long, TinyTaskWorker> pair : workers) {
        final TinyTaskWorker worker = pair.right();
        if (worker.canHandleTask()) {
          return worker;
        }
      }
    }

    throw new RuntimeException("No executable worker");
  }

  public synchronized TinyTaskWorker prepareSendTask() {
    //eventHandlerMap.put(offloadingTask.taskId, taskResultHandler);

    if (!offloadingTransformSerialized.get()) {

      offloadingTransformSerialized.set(true);

      final ByteBufOutputStream bos = new ByteBufOutputStream(workerInitBuffer);
      ObjectOutputStream oos = null;
      try {
        oos = new ObjectOutputStream(bos);
        oos.writeObject(offloadingTransform);
        oos.writeObject(offloadingSerializer.getInputDecoder());
        oos.writeObject(offloadingSerializer.getOutputEncoder());
        oos.close();
        bos.close();
      } catch (final IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    synchronized (workers) {
      for (final Pair<Long, TinyTaskWorker> pair : workers) {
        final TinyTaskWorker worker = pair.right();
        if (worker.prepareTaskIfPossible()) {
          LOG.info("There are preparable worker");
          return worker;
        }
      }

      final TinyTaskWorker newWorker = new TinyTaskWorker(
        createNewWorker(), evalConf);

      LOG.info("No preparable worker.. create new one {}", newWorker);

      workers.add(Pair.of(System.currentTimeMillis(), newWorker));

      newWorker.prepareTaskIfPossible();
      return newWorker;
    }
  }

  public TinyTaskWorker createWorker() {

    if (!offloadingTransformSerialized.get()) {


      final ByteBufOutputStream bos = new ByteBufOutputStream(workerInitBuffer);
      ObjectOutputStream oos = null;
      try {
        oos = new ObjectOutputStream(bos);
        oos.writeObject(offloadingTransform);
        oos.writeObject(offloadingSerializer.getInputDecoder());
        oos.writeObject(offloadingSerializer.getOutputEncoder());
        oos.close();
        bos.close();
      } catch (final IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      offloadingTransformSerialized.set(true);
    }


    final TinyTaskWorker newWorker = new TinyTaskWorker(
      createNewWorker(), evalConf);

    LOG.info("No preparable worker.. create new one {}", newWorker);

    synchronized (workers) {
      workers.add(Pair.of(System.currentTimeMillis(), newWorker));
    }

    newWorker.prepareTaskIfPossible();
    return newWorker;
  }

  public void sendReadyTask(final ReadyTask readyTask,
                            final TinyTaskWorker worker) {
    LOG.info("Send ready task {}", readyTask.taskId);
    worker.addReadyTask(readyTask);
  }

  public void sendTask(final OffloadingTask offloadingTask,
                       final TaskExecutor taskExecutor,
                       final TinyTaskWorker worker) {
    //eventHandlerMap.put(offloadingTask.taskId, taskResultHandler);
    //LOG.info("Put task {} to offloadedTaskMap {}", offloadingTask.taskId, taskExecutor);
    offloadedTaskMap.put(offloadingTask.taskId, taskExecutor);
    //LOG.info("Offloaded task map when put {}", offloadedTaskMap);
    worker.addTask(offloadingTask);
  }

  private TinyTaskWorker findWorkerThatHandleTask(final String taskId) {
    synchronized (workers) {
      for (final Pair<Long, TinyTaskWorker> pair : workers) {
        if (pair.right().hasTask(taskId)) {
          return pair.right();
        }
      }
    }

    throw new RuntimeException("No worker that handles task " + taskId);
  }

  public synchronized boolean deleteTask(final String taskId) {
    LOG.info("Delete task {}", taskId);
    final TinyTaskWorker worker = findWorkerThatHandleTask(taskId);
    if (!worker.deleteTask(taskId)) {
      worker.getDeletePending().getAndIncrement();
      deletePendingWorkers.put(taskId, worker);
      LOG.info("Put task {} to pending ... size: {}, deletePending: {}",
        taskId, deletePendingWorkers.size(), worker.getDeletePending());

      return false;
    } else {
      if (worker.hasNoTask() && worker.getDeletePending().get() == 0) {
        LOG.info("Closing worker for {}...", taskId);
        worker.close();
        removeRunningWorker(worker);
        deletePendingWorkers.remove(taskId);
      }

      return true;
    }
  }

  @Override
  public void execute(ByteBuf data) {
    throw new RuntimeException("Unsupported operation");
  }

  @Override
  public void execute(final String id, final ByteBuf data, final EventHandler<O> eventHandler) {
    throw new RuntimeException("Unsupported operation");
  }

  // This function is actually requesting tasks
  // createStreamWorker -> requestToSendTask
  @Override
  public OffloadingWorker createStreamWorker() {
    throw new RuntimeException("Unsupported operation");
  }

  @Override
  public void shutdown() {
  }

  @Override
  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public boolean isFinished() {
    return finished;
  }
}
