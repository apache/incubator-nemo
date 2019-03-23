package org.apache.nemo.offloading.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import org.apache.nemo.offloading.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * This should be called by one thread.
 * @param <I>
 * @param <O>
 */
public final class StreamingWorkerService<I, O> implements ServerlessExecutorService<I> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingWorkerService.class.getName());
  private final OffloadingWorkerFactory workerFactory;

  private final List<Pair<Long, OffloadingWorker>> streamingWorkers;

  // buffer that contains bytes for initializing workers
  private final ByteBuf workerInitBuffer;

  private final OffloadingSerializer<I, O> offloadingSerializer;

  private final EventHandler<O> eventHandler;

  private final AtomicInteger createdWorkers = new AtomicInteger(0);
  private int finishedWorkers = 0;

  //private final StatePartitioner<I, S> statePartitioner;
  //private final List<ByteBuf> states;
  //private final Map<Integer, OffloadingWorker<I, O>> stateIndexAndWorkerMap;

  private long totalProcessingTime = 0;
  private long totalWorkerInitTime = 0;
  private long workerInitCnt = 0;
  private int processingCnt = 0;

  //private int bufferedCnt = 0;
  //private int addedOutput = 0;

  private boolean finished = false;
  private volatile boolean shutdown = false;

  final AtomicLong st = new AtomicLong(System.currentTimeMillis());

  public StreamingWorkerService(
    final OffloadingWorkerFactory workerFactory,
    final OffloadingTransform offloadingTransform,
    final OffloadingSerializer<I, O> offloadingSerializer,
    final EventHandler<O> eventHandler) {

    LOG.info("Start cached pool serverless executor service");

    this.workerFactory = workerFactory;
    this.streamingWorkers = new LinkedList<>();

    this.workerInitBuffer = Unpooled.directBuffer();
    this.offloadingSerializer = offloadingSerializer;
    this.eventHandler = eventHandler;

    final ByteBufOutputStream bos = new ByteBufOutputStream(workerInitBuffer);
    ObjectOutputStream oos = null;
    try {
      oos = new ObjectOutputStream(bos);
      oos.writeObject(offloadingTransform);
      //LOG.info("WorkerInitBuffer - encode offloadingTransform: {}", workerInitBuffer.readableBytes());
      oos.writeObject(offloadingSerializer.getInputDecoder());
      //LOG.info("WorkerInitBuffer - encode offloadingSerializer.decoder: {}", workerInitBuffer.readableBytes());
      oos.writeObject(offloadingSerializer.getOutputEncoder());
      //LOG.info("WorkerInitBuffer - encode offloadingSerializer.encoder: {}", workerInitBuffer.readableBytes());

      oos.close();
      bos.close();
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void execute(I data) {
    throw new RuntimeException("Unsupported operation");
  }

  @Override
  public void execute(ByteBuf data) {
    throw new RuntimeException("Unsupported operation");
  }

  // we don't have to send data to streaming workers
  // because it will pull the data
  @Override
  public OffloadingWorker createStreamWorker() {
    createdWorkers.getAndIncrement();
    // create new worker
    //LOG.info("Create worker");
    final ByteBuf copiedBuf;
    synchronized (workerInitBuffer) {
      copiedBuf = workerInitBuffer.retainedDuplicate();
    }

    final OffloadingWorker<I, O> worker =
      workerFactory.createStreamingWorker(copiedBuf, offloadingSerializer, eventHandler);

    synchronized (streamingWorkers) {
      streamingWorkers.add(Pair.of(System.currentTimeMillis(), worker));
    }

    return worker;
  }

  @Override
  public void shutdown() {

    shutdown = true;
    // shutdown all workers
    long prevTime = System.currentTimeMillis();

    synchronized (streamingWorkers) {
      if (!streamingWorkers.isEmpty()) {
        LOG.info("Shutting down streaming workers: {}", streamingWorkers.size());
        for (final Pair<Long, OffloadingWorker> pair : streamingWorkers) {
          pair.right().finishOffloading();
        }
      }
    }

    // TODO: release worker init buffer
    workerInitBuffer.release();

    finished = true;
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
