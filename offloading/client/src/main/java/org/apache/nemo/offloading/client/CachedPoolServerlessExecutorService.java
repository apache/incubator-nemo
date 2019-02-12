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
  private final ExecutorService outputEmitterThread;

  private final EventHandler<O> eventHandler;

  private final List<ByteBuf> dataBufferList;

  private final PriorityQueue<Pair<Long, OffloadingWorker>> readyWorkers;

  private final BlockingQueue<Future<O>> outputQueue;

  private volatile boolean finished = false;

  //private final StatePartitioner<I, S> statePartitioner;
  //private final List<ByteBuf> states;
  //private final Map<Integer, OffloadingWorker<I, O>> stateIndexAndWorkerMap;

  CachedPoolServerlessExecutorService(
    final OffloadingWorkerFactory workerFactory,
    final OffloadingTransform offloadingTransform,
    final OffloadingSerializer<I, O> offloadingSerializer,
    final EventHandler<O> eventHandler) {
    this.workerFactory = workerFactory;
    this.readyWorkers = new PriorityQueue<>(new Comparator<Pair<Long, OffloadingWorker>>() {
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
    this.scheduler = Executors.newScheduledThreadPool(2);
    this.eventHandler = eventHandler;
    //this.statePartitioner = statePartitioner;
    //this.stateIndexAndWorkerMap = new HashMap<>();

    this.dataBufferList = new ArrayList<>();

    this.outputQueue = new LinkedBlockingQueue<>();
    this.outputEmitterThread = Executors.newSingleThreadExecutor();
    outputEmitterThread.execute(() -> {

      while (!Thread.currentThread().isInterrupted() && !finished) {
        try {
          final O output = outputQueue.take().get();
          eventHandler.onNext(output);
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    });

    scheduler.scheduleAtFixedRate(() -> {
      LOG.info("Init workers: {}, Running workers: {}, Ready workers: {} ",
        initializingWorkers.size(), runningWorkers.size(), readyWorkers.size());
    }, 1, 1, TimeUnit.SECONDS);

    // schedule init/active worker
    scheduler.scheduleAtFixedRate(() -> {

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

      // active workers
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
    }, 200, 200, TimeUnit.MILLISECONDS);

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

  private ByteBuf encodeData(final I data, final ByteBuf byteBuf) {
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

  private void createNewWorker(final I data) {
    // create new worker
    LOG.info("Create worker");
    workerInitBuffer.retain();
    final OffloadingWorker<I, O> worker =
      workerFactory.createOffloadingWorker(workerInitBuffer, offloadingSerializer);
    dataBufferList.add(encodeData(data, Unpooled.directBuffer()));
    initializingWorkers.add(worker);
  }

  private void executeBufferedData() {
    while (!readyWorkers.isEmpty() && !dataBufferList.isEmpty()) {
      final OffloadingWorker readyWorker = readyWorkers.poll().right();
      final ByteBuf buf = dataBufferList.remove(0);
      LOG.info("Execute data for worker {}, isReadY: {}", readyWorker, readyWorker.isReady());
      outputQueue.add(readyWorker.execute(buf));
      synchronized (runningWorkers) {
        // possible concurreny issue
        // (readyWorker may become "ready" before it is added to the runningWorkers )
        runningWorkers.add(readyWorker);
      }
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
        final OffloadingWorker worker = readyWorkers.poll().right();
        final ByteBuf encodedData =
          encodeData(data, worker.getChannel().alloc().ioBuffer());
        outputQueue.add(worker.execute(encodedData));
      } else {
        createNewWorker(data);
      }
    }
  }

  @Override
  public void shutdown() {
    // shutdown all workers
    while (!runningWorkers.isEmpty() || !initializingWorkers.isEmpty() || !readyWorkers.isEmpty()) {
      // handle buffered data
      while (!readyWorkers.isEmpty() && !dataBufferList.isEmpty()) {
        final OffloadingWorker readyWorker = readyWorkers.poll().right();
        final ByteBuf buf = dataBufferList.remove(0);
        LOG.info("BB Execute data for worker {}, isReadY: {}", readyWorker, readyWorker.isReady());
        outputQueue.add(readyWorker.execute(buf));
        synchronized (runningWorkers) {
          // possible concurreny issue
          // (readyWorker may become "ready" before it is added to the runningWorkers )
          runningWorkers.add(readyWorker);
        }
      }

      // finish ready workers if there are no buffered data
      while (!readyWorkers.isEmpty() && dataBufferList.isEmpty()) {
        final OffloadingWorker worker = readyWorkers.poll().right();
        worker.finishOffloading();
      }

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    finished = true;

    outputEmitterThread.shutdown();
    scheduler.shutdown();

    // TODO: release worker init buffer
    workerInitBuffer.release();
    scheduler.shutdown();
  }
}
