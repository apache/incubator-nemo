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
import java.util.concurrent.atomic.AtomicLong;

/**
 * This should be called by one thread.
 * @param <I>
 * @param <O>
 */
final class CachedPoolServerlessExecutorService<I, O> implements ServerlessExecutorService<I> {
  private static final Logger LOG = LoggerFactory.getLogger(CachedPoolServerlessExecutorService.class.getName());
  private final OffloadingWorkerFactory workerFactory;

  private final List<Pair<Long, OffloadingWorker>> initializingWorkers;

  // left: start time, right: worker
  private final List<Pair<Long, OffloadingWorker>> runningWorkers;

  // buffer that contains bytes for initializing workers
  private final ByteBuf workerInitBuffer;

  private final OffloadingSerializer<I, O> offloadingSerializer;

  private final ScheduledExecutorService scheduler;

  private final EventHandler<O> eventHandler;

  private final BlockingQueue<Pair<ByteBuf, Integer>> dataBufferQueue;

  //private final PriorityBlockingQueue<Pair<Long, OffloadingWorker>> readyWorkers;

  private final BlockingQueue<PendingOutput<O>> outputQueue;

  // key: data id, val: true (if the data is already processed by a worker)
  private final Map<Integer, Boolean> speculativeDataProcessedMap;

  private int createdWorkers = 0;
  private int finishedWorkers = 0;

  //private final StatePartitioner<I, S> statePartitioner;
  //private final List<ByteBuf> states;
  //private final Map<Integer, OffloadingWorker<I, O>> stateIndexAndWorkerMap;

  private long totalProcessingTime = 0;
  private int processingCnt = 0;

  private int bufferedCnt = 0;
  private int addedOutput = 0;

  final AtomicLong st = new AtomicLong(System.currentTimeMillis());

  CachedPoolServerlessExecutorService(
    final OffloadingWorkerFactory workerFactory,
    final OffloadingTransform offloadingTransform,
    final OffloadingSerializer<I, O> offloadingSerializer,
    final EventHandler<O> eventHandler) {

    LOG.info("Start cached pool serverless executor service");

    this.workerFactory = workerFactory;
    this.speculativeDataProcessedMap = new HashMap<>();
    this.initializingWorkers = new LinkedList<>();
    this.runningWorkers = new LinkedList<>();

    this.workerInitBuffer = Unpooled.directBuffer();
    this.offloadingSerializer = offloadingSerializer;
    this.scheduler = Executors.newScheduledThreadPool(1);
    this.eventHandler = eventHandler;
    //this.statePartitioner = statePartitioner;
    //this.stateIndexAndWorkerMap = new HashMap<>();

    this.dataBufferQueue = new LinkedBlockingQueue<>();
    this.outputQueue = new LinkedBlockingQueue<>();



    // schedule init/active worker
    this.scheduler.scheduleAtFixedRate(() -> {

      boolean logging = false;
      if (System.currentTimeMillis() - st.get() > 5000) {
        logging = true;
        st.set(System.currentTimeMillis());
        LOG.info("Init workers: {}, Running workers: {}, Output: {} ",
          initializingWorkers.size(), runningWorkers.size(), outputQueue.size());


        for (final Pair<Long, OffloadingWorker> pair : runningWorkers) {
          if (st.get() - pair.left() > 7000) {
            final Pair<ByteBuf, Integer> input = pair.right().getCurrentProcessingInput();
            if (input != null) {
              LOG.info("Running worker for data {}, time: {}", input.right(), (st.get() - pair.left()));
            }
          }
        }

        for (final PendingOutput output : outputQueue) {
          if (st.get() - output.startTime > 7000) {
            LOG.info("Pending output:  data {}, time: {}", output.dataId, (st.get() - output.startTime));
          }
        }
      }

      try {
        // initializing worker -> running workers

        synchronized (initializingWorkers) {
          final Iterator<Pair<Long, OffloadingWorker>> iterator = initializingWorkers.iterator();
          while (iterator.hasNext()) {
            final Pair<Long, OffloadingWorker> pair = iterator.next();
            if (pair.right().isReady()) {
              LOG.info("Init worker latency: {}", System.currentTimeMillis() - pair.left());
              iterator.remove();
              // do not add it to ready workers
              // instead, just execute data
              executeData(pair.right());
            }
          }
        }

        outputEmittion();

        long curTime = System.currentTimeMillis();
        final List<OffloadingWorker> readyWorkers = new ArrayList<>(runningWorkers.size());
        final Iterator<Pair<Long, OffloadingWorker>> iterator = runningWorkers.iterator();
        while (iterator.hasNext()) {
          final Pair<Long, OffloadingWorker> pair = iterator.next();
          if (pair.right().isReady()) {
            iterator.remove();
            readyWorkers.add(pair.right());

            totalProcessingTime += (curTime - pair.left());
            processingCnt += 1;

          } else if (isOutputEmitted(pair.right())) {
            // the output is already emitted
            // just finish this worker
            iterator.remove();
            final Pair<ByteBuf, Integer> data = pair.right().getCurrentProcessingInput();

            if (data == null) {
              // this is end
              readyWorkers.add(pair.right());

              totalProcessingTime += (curTime - pair.left());
              processingCnt += 1;

            } else {
              final int dataId = data.right();
              LOG.info("Reject execution for data: {}", dataId);
              finishedWorkers += 1;
              pair.right().finishOffloading();
            }
          }
        }

        readyWorkers.forEach(readyWorker -> {
          executeData(readyWorker);
        });

        //speculativeExecution();
        if (initializingWorkers.isEmpty() &&
          (finishedWorkers / (double) createdWorkers) > 0.6) {
          curTime = System.currentTimeMillis();
          final long avgTime = totalProcessingTime / processingCnt;
          for (final Pair<Long, OffloadingWorker> pair : runningWorkers) {
            if (!hasBeenPerformedSpeculativeExecution(pair.right()) &&
              curTime - pair.left() > avgTime * 2) {
              // speculative execution1!
              final OffloadingWorker runningWorker = pair.right();
              final Pair<ByteBuf, Integer> data = runningWorker.getCurrentProcessingInput();
              if (data != null) {
                final int dataId = data.right();

                LOG.info("Create new worker for speculatve execution of data {}, elapsed time: {}, avg time: {}"
                  , dataId, (curTime - pair.left()), avgTime);

                createdWorkers += 1;
                // create new worker
                //LOG.info("Create worker");
                final ByteBuf copiedBuf = workerInitBuffer.retainedDuplicate();

                dataBufferQueue.add(data);
                bufferedCnt += 1;

                final OffloadingWorker<I, O> worker =
                  workerFactory.createOffloadingWorker(copiedBuf, offloadingSerializer);

                speculativeDataProcessedMap.put(dataId, false);

                synchronized (initializingWorkers) {
                  initializingWorkers.add(Pair.of(System.currentTimeMillis(), worker));
                }
              }
            }
          }
        }


      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }, 300, 300, TimeUnit.MILLISECONDS);

    final ByteBufOutputStream bos = new ByteBufOutputStream(workerInitBuffer);
    ObjectOutputStream oos = null;
    try {
      LOG.info("WorkerInitBuffer - start: {}", workerInitBuffer.readableBytes());
      oos = new ObjectOutputStream(bos);
      oos.writeObject(offloadingTransform);
      LOG.info("WorkerInitBuffer - encode offloadingTransform: {}", workerInitBuffer.readableBytes());
      oos.writeObject(offloadingSerializer.getInputDecoder());
      LOG.info("WorkerInitBuffer - encode offloadingSerializer.decoder: {}", workerInitBuffer.readableBytes());
      oos.writeObject(offloadingSerializer.getOutputEncoder());
      LOG.info("WorkerInitBuffer - encode offloadingSerializer.encoder: {}", workerInitBuffer.readableBytes());

      /*
      if (statePartitioner != null) {
        oos.writeObject(statePartitioner.getStateEncoderFactory());
        oos.writeObject(statePartitioner.getStateDecoderFactory());
      }
      */

      oos.close();
      bos.close();
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    // state serialize
    /*
    if (statePartitioner != null) {
      final OffloadingEncoder<S> stateEncoderFactory = statePartitioner.getStateEncoderFactory();
      this.states = new ArrayList<>(statePartitioner.getStatePartition().size());
      for (final S state : statePartitioner.getStatePartition()) {
        final ByteBuf byteBuf = Unpooled.directBuffer();
        final ByteBufOutputStream bbos = new ByteBufOutputStream(byteBuf);
        try {
          final OffloadingEncoder.Encoder<S> encoder = stateEncoderFactory.create(bbos);
          encoder.encode(state);
          bbos.close();
          states.add(byteBuf);
        } catch (final IOException e) {
          e.printStackTrace();
        }
      }
    } else {
      this.states = null;
    }
    */
  }

  private boolean isOutputEmitted(final OffloadingWorker runningWorker) {
    final Pair<ByteBuf, Integer> curInput = runningWorker.getCurrentProcessingInput();
    if (curInput != null) {
      return speculativeDataProcessedMap.getOrDefault(curInput.right(), false);
    } else {
      return true;
    }
  }

  private boolean hasBeenPerformedSpeculativeExecution(final OffloadingWorker runningWorker) {
    final Pair<ByteBuf, Integer> curInput = runningWorker.getCurrentProcessingInput();
    if (curInput != null) {
      return speculativeDataProcessedMap.containsKey(curInput.right());
    } else {
      return true;
    }
  }

  private OffloadingWorker selectRunningWorkerForSpeculativeExecution(final OffloadingWorker readyWorker) {
    int cnt = Integer.MAX_VALUE;
    OffloadingWorker target = null;
    int cnt2 = Integer.MAX_VALUE;
    OffloadingWorker target2 = null;
    // first find a worker that does not perform speculative execution
    for (final Pair<Long, OffloadingWorker> runningWorkerPair : runningWorkers) {
      final OffloadingWorker runningWorker = runningWorkerPair.right();
      final int runningWorkerCnt = runningWorker.getDataProcessingCnt();
      if (cnt > runningWorkerCnt && !hasBeenPerformedSpeculativeExecution(runningWorker)
        && runningWorkerCnt + 1 < readyWorker.getDataProcessingCnt()) {
        target = runningWorker;
      }

      if (cnt2 > runningWorkerCnt && !isOutputEmitted(runningWorker)
        && runningWorkerCnt + 1 < readyWorker.getDataProcessingCnt()) {
        target2 = runningWorker;
      }
    }

    if (target == null) {
      return target2;
    } else {
      return target;
    }
  }

  private void executeData(final OffloadingWorker worker) {
    final Pair<ByteBuf, Integer> pair = dataBufferQueue.poll();
    if (pair != null) {
      final int dataId = pair.right();
      final ByteBuf dataBuf = pair.left();
      final boolean speculative = speculativeDataProcessedMap.containsKey(dataId);

      try {
        final PendingOutput po = new PendingOutput(worker.execute(dataBuf, dataId, speculative), dataId);
        outputQueue.add(po);
        addedOutput += 1;
        runningWorkers.add(Pair.of(System.currentTimeMillis(), worker));
      } catch (final IllegalReferenceCountException e) {
        // the input becomes null ... this means that we don't have to do speculative execution
        // just finish the worker!
        LOG.info("Illegal reference count exception for executing data {}... just finis worker", dataId);
        worker.finishOffloading();
        finishedWorkers += 1;
      }
    } else {
      // speculative execution
      //speculativeExecution(worker);
      worker.finishOffloading();
      finishedWorkers += 1;
    }
  }

  private void speculativeExecution(final OffloadingWorker readyWorker) {
    // speculative execution when there are ready workers
    final OffloadingWorker runningWorker = selectRunningWorkerForSpeculativeExecution(readyWorker);
    if (runningWorker == null) {
      // just end the worker
      readyWorker.finishOffloading();
      finishedWorkers += 1;
    } else {
      final Pair<ByteBuf, Integer> data = runningWorker.getCurrentProcessingInput();
      if (data != null) {
        final int dataId = data.right();


        try {
          outputQueue.add(new PendingOutput<>(readyWorker.execute(data.left(), dataId, true), dataId));
          addedOutput += 1;

          // TODO: consideration
          LOG.info("Speculative execution for data {}, runningWorkerCnt: {}, readyWorkerCnt: {}", dataId,
            runningWorker.getDataProcessingCnt(), readyWorker.getDataProcessingCnt());

          if (!speculativeDataProcessedMap.containsKey(dataId)) {
            speculativeDataProcessedMap.put(dataId, false);
          }

          runningWorkers.add(Pair.of(System.currentTimeMillis(), readyWorker));
        } catch (final IllegalReferenceCountException e) {
          // the input becomes null ... this means that we don't have to do speculative execution
          // just finish the worker!
          LOG.info("Illegal reference count exception for executing data {}... just finis worker", dataId);
          readyWorker.finishOffloading();
          finishedWorkers += 1;
        }
      }
    }
  }

  private void outputEmittion() {
    // emit output
    final Iterator<PendingOutput<O>> iterator = outputQueue.iterator();
    while (iterator.hasNext()) {
      try {
        final PendingOutput<O> output = iterator.next();
        final int dataId = output.dataId;
        final Future<Optional<O>> data = output.output;

        if (speculativeDataProcessedMap.getOrDefault(dataId, false)) {
          // this is already emitted. just remove
          iterator.remove();
        } else if (data.isDone()) {

          boolean isEmittable = !speculativeDataProcessedMap.getOrDefault(dataId, false);

          speculativeDataProcessedMap.put(dataId, true);
          LOG.info("Speculative execution output emittion, data: {}, emittable: {}", dataId,
            isEmittable);

          LOG.info("Output latency {}, id {} done", System.currentTimeMillis() - output.startTime, dataId);
          iterator.remove();
          final Optional<O> optional = data.get();
          if (isEmittable && optional.isPresent()) {
            LOG.info("Output receive: {}", optional.get());
            eventHandler.onNext(optional.get());
          }
        }

      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  private ByteBuf encodeData(final I data, final ByteBuf byteBuf) {

    final ByteBufOutputStream dataBos = new ByteBufOutputStream(byteBuf);
    try {
      offloadingSerializer.getInputEncoder().encode(data, dataBos);
      dataBos.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    return dataBos.buffer();
  }

  // init worker
  private void createNewWorker(final I data) {
    createdWorkers += 1;
    // create new worker
    //LOG.info("Create worker");
    final ByteBuf copiedBuf = workerInitBuffer.retainedDuplicate();

    dataBufferQueue.add(Pair.of(encodeData(data, Unpooled.directBuffer()),
      workerFactory.getAndIncreaseDataId()));
    bufferedCnt += 1;

    final OffloadingWorker<I, O> worker =
      workerFactory.createOffloadingWorker(copiedBuf, offloadingSerializer);

    synchronized (initializingWorkers) {
      initializingWorkers.add(Pair.of(System.currentTimeMillis(), worker));
    }
  }


  // init worker
  private void createNewWorker(final ByteBuf data) {
    createdWorkers += 1;
    // create new worker
    //LOG.info("Create worker");
    final ByteBuf copiedBuf = workerInitBuffer.retainedDuplicate();
    dataBufferQueue.add(Pair.of(data, workerFactory.getAndIncreaseDataId()));
    bufferedCnt += 1;

    final OffloadingWorker<I, O> worker =
      workerFactory.createOffloadingWorker(copiedBuf, offloadingSerializer);

    synchronized (initializingWorkers) {
      initializingWorkers.add(Pair.of(System.currentTimeMillis(), worker));
    }
  }

  @Override
  public void execute(I data) {
    createNewWorker(data);
  }

  @Override
  public void execute(ByteBuf data) {
    createNewWorker(data);
  }

  @Override
  public void shutdown() {
    // shutdown all workers
    long prevTime = System.currentTimeMillis();
    LOG.info("Shutting down workers {}/{}..., init: {}, running: {}", finishedWorkers, createdWorkers);

    while (finishedWorkers < createdWorkers) {
      // logging
      if (System.currentTimeMillis() - prevTime > 2000) {
        prevTime = System.currentTimeMillis();
        LOG.info("Shutting down workers {}/{}... scheduler is shutdown: {}, is terminated: {}, triggerTime: {}",
          finishedWorkers, createdWorkers, scheduler.isShutdown(), scheduler.isTerminated(),
          st.get());
      }

      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // waiting for all outputs
    LOG.info("Waiting {} outputs... {}", outputQueue.size(), outputQueue);
    while (!outputQueue.isEmpty()) {
      // logging
      if (System.currentTimeMillis() - prevTime > 2000) {
        prevTime = System.currentTimeMillis();
        LOG.info("Waiting {} outputs... {}", outputQueue.size(), outputQueue);
        LOG.info("Created worker: {}, Finished worker: {}, added output: {}", createdWorkers, finishedWorkers);
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    LOG.info("Shutting down done");

    scheduler.shutdown();

    // TODO: release worker init buffer
    workerInitBuffer.release();
  }

  final class PendingOutput<O> {
    private final Future<Optional<O>> output;
    private final int dataId;
    private final long startTime;

    public PendingOutput(final Future<Optional<O>> output,
                         final int dataId) {
      this.output = output;
      this.dataId = dataId;
      this.startTime = System.currentTimeMillis();
    }
  }
}
