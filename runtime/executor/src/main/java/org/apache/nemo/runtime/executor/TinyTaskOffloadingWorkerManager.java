package org.apache.nemo.runtime.executor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.offloading.client.StreamingLambdaWorkerProxy;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingHeartbeatEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultEvent;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;
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
  private final ByteBuf workerInitBuffer;

  private final OffloadingSerializer<I, O> offloadingSerializer;

  private final ConcurrentMap<String, EventHandler<Object>> eventHandlerMap;

  private boolean finished = false;
  private volatile boolean shutdown = false;

  final AtomicLong st = new AtomicLong(System.currentTimeMillis());

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  private final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder;

  private final ConcurrentMap<String, TaskExecutor> offloadedTaskMap = new ConcurrentHashMap<>();

  public TinyTaskOffloadingWorkerManager(
    final OffloadingWorkerFactory workerFactory,
    final OffloadingTransform offloadingTransform,
    final OffloadingSerializer<I, O> offloadingSerializer,
    final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder) {

    LOG.info("Start cached pool serverless executor service");

    this.eventHandlerMap = new ConcurrentHashMap<>();
    this.checkpointMarkCoder = checkpointMarkCoder;

    this.workerFactory = workerFactory;
    this.workers = new LinkedList<>();

    this.workerInitBuffer = Unpooled.directBuffer();
    this.offloadingSerializer = offloadingSerializer;

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


    scheduler.scheduleAtFixedRate(() -> {
      // scheduling
      for (final Pair<Long, TinyTaskWorker> pair : workers) {
        pair.right().executePending();
      }

    }, 1, 1, TimeUnit.SECONDS);
  }

  public synchronized void sendTask(final OffloadingTask offloadingTask,
                                    final TaskExecutor taskExecutor) {
    //eventHandlerMap.put(offloadingTask.taskId, taskResultHandler);
    offloadedTaskMap.put(offloadingTask.taskId, taskExecutor);

    // find worker
    if (workers.size() == 0) {
      // create new worker
      final ByteBuf copiedBuf;
      synchronized (workerInitBuffer) {
        copiedBuf = workerInitBuffer.retainedDuplicate();
      }

      final StreamingLambdaWorkerProxy worker = (StreamingLambdaWorkerProxy)
        workerFactory.createStreamingWorker(copiedBuf, offloadingSerializer, (event) -> {
          // TODO: We should retrieve states (checkpointmark, operator states, and so on)

          final Pair<String, Object> pair = (Pair<String, Object>) event;
          final Object msg = pair.right();
          LOG.info("Receive data for {} / {}", pair.left(), pair.right());

          final TaskExecutor te = offloadedTaskMap.get(pair.left());

          if (msg instanceof OffloadingHeartbeatEvent) {
            final OffloadingHeartbeatEvent heartbeatEvent = (OffloadingHeartbeatEvent) msg;
            te.setOffloadedTaskTime(heartbeatEvent.time);

          } else if (msg instanceof OffloadingResultEvent) {
            if (((OffloadingResultEvent) msg).data.size() > 0) {
              //LOG.info("Result received: cnt {}", ((OffloadingResultEvent) msg).data.size());
              te.getOffloadingQueue().add(msg);
            }
          } else {
            te.getOffloadingQueue().add(msg);
          }
        });

        workers.add(Pair.of(System.currentTimeMillis(), new TinyTaskWorker(worker, checkpointMarkCoder)));
    }

    final int index = workers.size() - 1;
    final TinyTaskWorker worker = workers.get(index).right();

    if (worker.canHandleTask()) {
      worker.addTask(offloadingTask);
    }
  }

  private TinyTaskWorker findWorkerThatHandleTask(final String taskId) {
    for (final Pair<Long, TinyTaskWorker> pair : workers) {
      if (pair.right().hasTask(taskId)) {
        return pair.right();
      }
    }

    throw new RuntimeException("No worker that handles task " + taskId);
  }

  public synchronized void deleteTask(final OffloadingTask task) {
    final TinyTaskWorker worker = findWorkerThatHandleTask(task.taskId);
    worker.deleteTask(task.taskId);

    /*
    if (worker.hasNoTask()) {
      worker.close();
    }
    */
  }

  @Override
  public void execute(I data) {
    throw new RuntimeException("Unsupported operation");
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
