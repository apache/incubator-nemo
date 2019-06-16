package org.apache.nemo.runtime.executor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.common.Pair;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.client.StreamingLambdaWorkerProxy;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingHeartbeatEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultEvent;
import org.apache.nemo.runtime.lambdaexecutor.StateOutput;
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

  public TinyTaskOffloadingWorkerManager(
    final OffloadingWorkerFactory workerFactory,
    final OffloadingTransform offloadingTransform,
    final EvalConf evalConf) {

    this.evalConf = evalConf;

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

  private StreamingLambdaWorkerProxy createNewWorker(
    final OffloadingSerializer<I, O> offloadingSerializer) {

    final ByteBuf workerInitBuffer = PooledByteBufAllocator.DEFAULT.buffer();
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

    // create new worker
    LOG.info("Creating new worker... current num: {}", workers.size());

    final StreamingLambdaWorkerProxy worker = (StreamingLambdaWorkerProxy)
      workerFactory.createStreamingWorker(workerInitBuffer, offloadingSerializer, (event) -> {
        // TODO: We should retrieve states (checkpointmark, operator states, and so on)

        final Pair<String, Object> pair = (Pair<String, Object>) event;
        final Object msg = pair.right();
        //LOG.info("Receive data for {} / {}", pair.left(), pair.right());

        final TaskExecutor te = offloadedTaskMap.get(pair.left());

        if (msg instanceof OffloadingHeartbeatEvent) {
          final OffloadingHeartbeatEvent heartbeatEvent = (OffloadingHeartbeatEvent) msg;
          //LOG.info("Set heartbeat for task {} / time {}", heartbeatEvent.taskId, heartbeatEvent.time);
          te.setOffloadedTaskTime(heartbeatEvent.time);

        } else if (msg instanceof OffloadingResultEvent) {
          if (((OffloadingResultEvent) msg).data.size() > 0) {
            //LOG.info("Result received: cnt {}", ((OffloadingResultEvent) msg).data.size());
            te.getOffloadingQueue().add(msg);
          }
        } else if (msg instanceof KafkaOffloadingOutput || msg instanceof StateOutput) {
          // End of the task!

          final TinyTaskWorker taskWorker = deletePendingWorkers.remove(te.getId());
          LOG.info("Pending Output: {}, delePendingWorker: {}, hasNoTask: {}, deletePending: {}, pendingWorkers: {}",
            te.getId(), taskWorker, taskWorker.hasNoTask(), taskWorker.getDeletePending(), deletePendingWorkers);

          final int pendingCnt = taskWorker.getDeletePending().decrementAndGet();

          if (taskWorker.hasNoTask() && pendingCnt == 0) {
            //taskWorker.close();
            //removeRunningWorker(taskWorker);
          }

          te.getOffloadingQueue().add(msg);
        } else {

          te.getOffloadingQueue().add(msg);
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

  public synchronized TinyTaskWorker prepareSendTask(
    final OffloadingSerializer<I, O> offloadingSerializer) {
    //eventHandlerMap.put(offloadingTask.taskId, taskResultHandler);

    synchronized (workers) {
      for (final Pair<Long, TinyTaskWorker> pair : workers) {
        final TinyTaskWorker worker = pair.right();
        if (worker.prepareTaskIfPossible()) {
          LOG.info("There are preparable worker");
          return worker;
        }
      }


      final TinyTaskWorker newWorker = new TinyTaskWorker(
        createNewWorker(offloadingSerializer), evalConf);

      LOG.info("No preparable worker.. create new one {}", newWorker);

      workers.add(Pair.of(System.currentTimeMillis(), newWorker));

      newWorker.prepareTaskIfPossible();
      return newWorker;
    }
  }

  public synchronized void sendTask(final OffloadingTask offloadingTask,
                                    final TaskExecutor taskExecutor,
                                    final TinyTaskWorker worker) {
    //eventHandlerMap.put(offloadingTask.taskId, taskResultHandler);
    offloadedTaskMap.put(offloadingTask.taskId, taskExecutor);
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
        //worker.close();
        //removeRunningWorker(worker);
        //deletePendingWorkers.remove(taskId);
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
