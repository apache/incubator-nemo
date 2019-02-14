package org.apache.nemo.offloading.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import org.apache.nemo.common.*;
import org.apache.nemo.common.coder.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.ref.Reference;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This should be called by one thread.
 * @param <I>
 * @param <O>
 */
@NotThreadSafe
final class CachedPoolServerlessExecutorService<I, O> implements ServerlessExecutorService<I> {
  private static final Logger LOG = LoggerFactory.getLogger(CachedPoolServerlessExecutorService.class.getName());
  private final OffloadingWorkerFactory workerFactory;

  private final List<OffloadingWorker> initializingWorkers;

  // left: start time, right: worker
  private final List<Pair<Long, OffloadingWorker>> runningWorkers;

  // buffer that contains bytes for initializing workers
  private final ByteBuf workerInitBuffer;

  private final OffloadingSerializer<I, O> offloadingSerializer;

  private final ScheduledExecutorService scheduler;

  private final EventHandler<O> eventHandler;

  private final BlockingQueue<ByteBuf> dataBufferQueue;

  //private final PriorityBlockingQueue<Pair<Long, OffloadingWorker>> readyWorkers;

  private final BlockingQueue<PendingOutput<O>> outputQueue;

  // key: data id, val: # of speculative executions
  private final Map<Integer, Integer> speculativeDataCounterMap;
  // key: data id, val: true (if the data is already processed by a worker)
  private final Map<Integer, Boolean> speculativeDataProcessedMap;

  private int createdWorkers = 0;
  private int finishedWorkers = 0;

  //private final StatePartitioner<I, S> statePartitioner;
  //private final List<ByteBuf> states;
  //private final Map<Integer, OffloadingWorker<I, O>> stateIndexAndWorkerMap;

  private long totalProcessingTime = 0;
  private int processingCnt = 0;

  CachedPoolServerlessExecutorService(
    final OffloadingWorkerFactory workerFactory,
    final OffloadingTransform offloadingTransform,
    final OffloadingSerializer<I, O> offloadingSerializer,
    final EventHandler<O> eventHandler) {

    LOG.info("Start cached pool serverless executor service");

    this.workerFactory = workerFactory;
    this.speculativeDataCounterMap = new HashMap<>();
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


    final AtomicLong st = new AtomicLong(System.currentTimeMillis());
    // schedule init/active worker
    this.scheduler.scheduleAtFixedRate(() -> {

      if (System.currentTimeMillis() - st.get() > 2000) {
        st.set(System.currentTimeMillis());
        LOG.info("Init workers: {}, Running workers: {}, Output: {} ",
          initializingWorkers.size(), runningWorkers.size(), outputQueue.size());
      }

      try {
        // initializing worker -> running workers
        synchronized (initializingWorkers) {
          final Iterator<OffloadingWorker> iterator = initializingWorkers.iterator();
          while (iterator.hasNext()) {
            final OffloadingWorker worker = iterator.next();
            if (worker.isReady()) {
              iterator.remove();
              // do not add it to ready workers
              // instead, just execute data
              executeData(worker);
            }
          }
        }

        // Reschedule running worker if it becomes ready
        final List<OffloadingWorker> readyWorkers = new ArrayList<>(runningWorkers.size());
        final Iterator<Pair<Long, OffloadingWorker>> iterator = runningWorkers.iterator();
        while (iterator.hasNext()) {
          final OffloadingWorker worker = iterator.next().right();
          if (worker.isReady()) {
            iterator.remove();
            readyWorkers.add(worker);
          }
        }

        // Reschedule ready workers
        readyWorkers.forEach(readyWorker -> {
          executeData(readyWorker);
        });


        outputEmittion();
        //speculativeExecution();


      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }, 300, 300, TimeUnit.MILLISECONDS);

    final ByteBufOutputStream bos = new ByteBufOutputStream(workerInitBuffer);
    this.workerInitBuffer.writeInt(NemoEvent.Type.WORKER_INIT.ordinal());
    ObjectOutputStream oos = null;
    try {
      oos = new ObjectOutputStream(bos);
      oos.writeObject(offloadingTransform);
      oos.writeObject(offloadingSerializer.getInputDecoder());
      oos.writeObject(offloadingSerializer.getOutputEncoder());

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
      final EncoderFactory<S> stateEncoderFactory = statePartitioner.getStateEncoderFactory();
      this.states = new ArrayList<>(statePartitioner.getStatePartition().size());
      for (final S state : statePartitioner.getStatePartition()) {
        final ByteBuf byteBuf = Unpooled.directBuffer();
        final ByteBufOutputStream bbos = new ByteBufOutputStream(byteBuf);
        try {
          final EncoderFactory.Encoder<S> encoder = stateEncoderFactory.create(bbos);
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

  private void executeData(final OffloadingWorker worker) {
    final ByteBuf dataBuf = dataBufferQueue.poll();
    if (dataBuf != null) {
      final int dataId = workerFactory.getAndIncreaseDataId();
      outputQueue.add(new PendingOutput(worker.execute(dataBuf, dataId), dataId));
      runningWorkers.add(Pair.of(System.currentTimeMillis(), worker));
    } else {
      // just end the worker
      worker.finishOffloading();
      finishedWorkers += 1;

      // speculative execution
      //speculativeExecution(worker);
    }
  }

  /*
  private void speculativeExecution(final OffloadingWorker readyWorker) {
    // speculative execution when there are ready workers
    if (!runningWorkers.isEmpty()) {
      synchronized (runningWorkers) {
        final List<OffloadingWorker> readyToRunningWorkers = new ArrayList<>(runningWorkers.size());

        for (final OffloadingWorker runningWorker : runningWorkers) {
          final Pair<Long, OffloadingWorker> readyWorkerPair = readyWorkers.poll();
          if (readyWorkerPair == null) {
            // exit speculative execution if there are no ready workers
            break;
          } else {
            final Pair<Integer, ByteBuf> data = runningWorker.getCurrentProcessingInput();
            if (data != null) {

              // TODO: consideration
              // 여기서 ByteBuf가 release 될수도 있음 (기존의 running worker에서 execution을 끝냈을 경우)
              final int cnt = speculativeDataCounterMap.getOrDefault(data.left(), 0);
              speculativeDataCounterMap.put(data.left(), cnt + 1);

              LOG.info("Speculative execution for data {}, cnt: {}", data.left(),
                speculativeDataCounterMap.get(data.left()));

              if (!speculativeDataProcessedMap.containsKey(data.left())) {
                speculativeDataProcessedMap.put(data.left(), false);
              }

              final OffloadingWorker readyWorker = readyWorkerPair.right();
              outputQueue.add(Pair.of(data.left(), readyWorker.execute(data.right(), data.left())));
              readyToRunningWorkers.add(readyWorker);
            }
          }
        }

        if (!readyToRunningWorkers.isEmpty()) {
          runningWorkers.addAll(readyToRunningWorkers);
        }
      }
    } else {
      // just end the worker
      readyWorker.finishOffloading();
      finishedWorkers += 1;
    }
  }
  */

  private void outputEmittion() {
    // emit output
    final Iterator<PendingOutput<O>> iterator = outputQueue.iterator();
    while (iterator.hasNext()) {
      try {
        final PendingOutput<O> output = iterator.next();
        boolean isEmittable = true;
        final int dataId = output.dataId;
        final Future<Optional<O>> data = output.output;

        if (speculativeDataCounterMap.containsKey(dataId)) {
          // speculative execution result
          if (!speculativeDataProcessedMap.get(dataId)) {
            // if the output is the first output among speculative execution
            // set true
            speculativeDataProcessedMap.put(dataId, true);
          } else {
            // this result is already emitted
            // TODO: reject execution
            isEmittable = false;
            iterator.remove();
          }

          final int count = speculativeDataCounterMap.get(dataId) - 1;

          LOG.info("Speculative execution output emittion, data: {}, cnt: {}", dataId,
            count);

          if (count <= 0) {
            speculativeDataCounterMap.remove(dataId);
            speculativeDataProcessedMap.remove(dataId);
          }
        }

        if (isEmittable) {
          if (data.isDone()) {
            LOG.info("Output latency {}, id {} done", System.currentTimeMillis() - output.startTime, dataId);
            iterator.remove();
            final Optional<O> optional = data.get();
            if (optional.isPresent()) {
              LOG.info("Output receive: {}", optional.get());
              eventHandler.onNext(optional.get());
            }
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
    byteBuf.writeInt(NemoEvent.Type.DATA.ordinal());

    final ByteBufOutputStream dataBos = new ByteBufOutputStream(byteBuf);
    try {
      final EncoderFactory.Encoder<I> encoder =
        offloadingSerializer.getInputEncoder().create(dataBos);
      encoder.encode(data);
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
    workerInitBuffer.retain();

    dataBufferQueue.add(encodeData(data, Unpooled.directBuffer()));

    final OffloadingWorker<I, O> worker =
      workerFactory.createOffloadingWorker(workerInitBuffer, offloadingSerializer);

    synchronized (initializingWorkers) {
      initializingWorkers.add(worker);
    }
  }

  @Override
  public void execute(I data) {
    createNewWorker(data);
  }

  @Override
  public void shutdown() {
    // shutdown all workers
    long prevTime = System.currentTimeMillis();
    LOG.info("Shutting down workers {}/{}...", finishedWorkers, createdWorkers);

    while (finishedWorkers < createdWorkers) {
      // logging
      if (System.currentTimeMillis() - prevTime > 2000) {
        prevTime = System.currentTimeMillis();
        LOG.info("Shutting down workers {}/{}...", finishedWorkers, createdWorkers);
      }

      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // waiting for all outputs
    LOG.info("Waiting {} outputs...", outputQueue.size());
    while (!outputQueue.isEmpty()) {
      // logging
      if (System.currentTimeMillis() - prevTime > 2000) {
        prevTime = System.currentTimeMillis();
        LOG.info("Waiting {} outputs...", outputQueue.size());
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
