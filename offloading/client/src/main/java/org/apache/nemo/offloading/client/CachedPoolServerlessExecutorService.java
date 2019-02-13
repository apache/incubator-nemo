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
import java.util.*;
import java.util.concurrent.*;

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
  private final List<OffloadingWorker> runningWorkers;

  // buffer that contains bytes for initializing workers
  private final ByteBuf workerInitBuffer;

  private final OffloadingSerializer<I, O> offloadingSerializer;

  private final ScheduledExecutorService scheduler;

  private final EventHandler<O> eventHandler;

  private final List<ByteBuf> dataBufferList;

  private final PriorityBlockingQueue<Pair<Long, OffloadingWorker>> readyWorkers;

  private final BlockingQueue<Pair<Integer, Future<Optional<O>>>> outputQueue;

  // key: data id, val: # of speculative executions
  private final Map<Integer, Integer> speculativeDataCounterMap;
  // key: data id, val: true (if the data is already processed by a worker)
  private final Map<Integer, Boolean> speculativeDataProcessedMap;

  //private final StatePartitioner<I, S> statePartitioner;
  //private final List<ByteBuf> states;
  //private final Map<Integer, OffloadingWorker<I, O>> stateIndexAndWorkerMap;

  CachedPoolServerlessExecutorService(
    final OffloadingWorkerFactory workerFactory,
    final OffloadingTransform offloadingTransform,
    final OffloadingSerializer<I, O> offloadingSerializer,
    final EventHandler<O> eventHandler) {
    this.workerFactory = workerFactory;
    this.speculativeDataCounterMap = new HashMap<>();
    this.speculativeDataProcessedMap = new HashMap<>();
    this.readyWorkers = new PriorityBlockingQueue<>(100, new Comparator<Pair<Long, OffloadingWorker>>() {
      @Override
      public int compare(Pair<Long, OffloadingWorker> o1, Pair<Long, OffloadingWorker> o2) {
        if (o1.left() < o2.left()) {
          return 1;
        } else if (o1.left() > o2.left()) {
          return -1;
        } else {
          return 0;
        }
      }
    });
    this.initializingWorkers = new LinkedList<>();
    this.runningWorkers = new LinkedList<>();
    this.workerInitBuffer = Unpooled.directBuffer();
    this.offloadingSerializer = offloadingSerializer;
    this.scheduler = Executors.newScheduledThreadPool(1);
    this.eventHandler = eventHandler;
    //this.statePartitioner = statePartitioner;
    //this.stateIndexAndWorkerMap = new HashMap<>();

    this.dataBufferList = new ArrayList<>();
    this.outputQueue = new LinkedBlockingQueue<>();

    this.scheduler.scheduleAtFixedRate(() -> {
      try {
        LOG.info("Init workers: {}, Running workers: {}, Ready workers: {} ",
          initializingWorkers.size(), runningWorkers.size(), readyWorkers.size());
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }, 1, 1, TimeUnit.SECONDS);

    // schedule init/active worker
    this.scheduler.scheduleAtFixedRate(() -> {

      try {

        // initializing worker -> ready workers
        synchronized (initializingWorkers) {
          final Iterator<OffloadingWorker> iterator = initializingWorkers.iterator();
          while (iterator.hasNext()) {
            final OffloadingWorker worker = iterator.next();
            if (worker.isReady()) {
              iterator.remove();
              readyWorkers.add(Pair.of(System.currentTimeMillis(), worker));
            }
          }
        }

        // running workers -> ready workers
        synchronized (runningWorkers) {
          final Iterator<OffloadingWorker> iterator = runningWorkers.iterator();
          while (iterator.hasNext()) {
            final OffloadingWorker worker = iterator.next();
            if (worker.isReady()) {
              iterator.remove();
              readyWorkers.add(Pair.of(System.currentTimeMillis(), worker));
            }
          }
        }

        outputEmittion();
        speculativeExecution();

      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }, 100, 100, TimeUnit.MILLISECONDS);

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

  private void speculativeExecution() {

    // speculative execution when there are ready workers
    if (!readyWorkers.isEmpty() && !runningWorkers.isEmpty()) {
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
    }
  }

  private void outputEmittion() {
    // emit output
    final Iterator<Pair<Integer, Future<Optional<O>>>> iterator = outputQueue.iterator();
    while (iterator.hasNext()) {
      try {
        final Pair<Integer, Future<Optional<O>>> output = iterator.next();
        boolean isEmittable = true;
        final int dataId = output.left();
        final Future<Optional<O>> data = output.right();

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
          if (data.isDone() && data.get().isPresent()) {
            iterator.remove();
            LOG.info("Output receive: {}", data);
            eventHandler.onNext(data.get().get());
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
    // create new worker
    //LOG.info("Create worker");
    workerInitBuffer.retain();
    final OffloadingWorker<I, O> worker =
      workerFactory.createOffloadingWorker(workerInitBuffer, offloadingSerializer);
    dataBufferList.add(encodeData(data, Unpooled.directBuffer()));

    synchronized (initializingWorkers) {
      initializingWorkers.add(worker);
    }
  }

  // ready -> running
  private void readyToRunning(final ByteBuf buf,
                              final Pair<Long, OffloadingWorker> readyWorkerPair) {
    if (readyWorkerPair != null) {
      final OffloadingWorker readyWorker = readyWorkerPair.right();
      final int dataId = workerFactory.getAndIncreaseDataId();
      //LOG.info("Execute data for worker {}, isReadY: {}", readyWorker, readyWorker.isReady());
      outputQueue.add(Pair.of(dataId, readyWorker.execute(buf, dataId)));
      synchronized (runningWorkers) {
        runningWorkers.add(readyWorker);
      }
    }
  }

  private void executeBufferedData() {
    while (!readyWorkers.isEmpty() && !dataBufferList.isEmpty()) {
      final Pair<Long, OffloadingWorker> readyWorkerPair = readyWorkers.poll();
      final ByteBuf buf = dataBufferList.remove(0);
      readyToRunning(buf, readyWorkerPair);
    }
  }

  @Override
  public void execute(I data) {
    if (readyWorkers.isEmpty()) {
      createNewWorker(data);
    } else {
      executeBufferedData();
      // select a worker
      if (!readyWorkers.isEmpty()) {
        // process current input data
        final Pair<Long, OffloadingWorker> readyWorkerPair = readyWorkers.poll();
        if (readyWorkerPair != null) {
          final ByteBuf encodedData =
            encodeData(data, readyWorkerPair.right().getChannel().alloc().ioBuffer());
          readyToRunning(encodedData, readyWorkerPair);
        }
      } else {
        createNewWorker(data);
      }
    }
  }

  @Override
  public void shutdown() {
    LOG.info("Shutting down workers...");
    // shutdown all workers
    while (!runningWorkers.isEmpty() || !initializingWorkers.isEmpty() || !readyWorkers.isEmpty()) {
      // handle buffered data
      while (!readyWorkers.isEmpty() && !dataBufferList.isEmpty()) {
        final Pair<Long, OffloadingWorker> readyWorkerPair = readyWorkers.poll();
        final ByteBuf buf = dataBufferList.remove(0);
        readyToRunning(buf, readyWorkerPair);
      }

      // finish ready workers if there are no buffered data
      while (!readyWorkers.isEmpty() && dataBufferList.isEmpty()) {
        final Pair<Long, OffloadingWorker> readyWorkerPair = readyWorkers.poll();
        if (readyWorkerPair != null) {
          readyWorkerPair.right().finishOffloading();
        }
      }

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // waiting for all outputs
    if (!outputQueue.isEmpty()) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    scheduler.shutdown();

    // TODO: release worker init buffer
    workerInitBuffer.release();
    scheduler.shutdown();
  }
}
