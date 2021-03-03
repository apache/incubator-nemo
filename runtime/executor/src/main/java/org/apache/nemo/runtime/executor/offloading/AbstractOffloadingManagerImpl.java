package org.apache.nemo.runtime.executor.offloading;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.coder.FSTSingleton;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.executor.PipeIndexMapWorker;
import org.apache.nemo.runtime.executor.TaskExecutorMapWrapper;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.executor.common.controlmessages.offloading.SendToOffloadingWorker;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingExecutor;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingExecutorSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.nemo.runtime.executor.common.OffloadingExecutorEventType.EventType.TASK_START;


public abstract class AbstractOffloadingManagerImpl implements OffloadingManager {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractOffloadingManagerImpl.class.getName());

  // private final List<OffloadingWorker> workers;
  protected final List<OffloadingWorker> workers;
  protected final OffloadingWorkerFactory workerFactory;
  protected final ConcurrentMap<String, List<OffloadingWorker>> taskWorkerMap = new ConcurrentHashMap<>();
  protected final ConcurrentMap<OffloadingWorker, List<String>> workerTaskMap = new ConcurrentHashMap<>();

  protected final TaskExecutorMapWrapper taskExecutorMapWrapper;
  protected final EvalConf evalConf;

  private final String executorId;
  protected final PipeIndexMapWorker pipeIndexMapWorker;
  private final ByteBuf offloadExecutorByteBuf;

  private final ExecutorService offloadingManagerThread;
  private volatile boolean isFinished = false;

  protected final AtomicLong currBufferedData = new AtomicLong(0);

  private boolean noPartialOffloading;

  private final boolean destroyOffloadingWorker;

  private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

  public AbstractOffloadingManagerImpl(final OffloadingWorkerFactory workerFactory,
                                       final TaskExecutorMapWrapper taskExecutorMapWrapper,
                                       final EvalConf evalConf,
                                       final PipeIndexMapWorker pipeIndexMapWorker,
                                       final String executorId,
                                       final String address,
                                       final int nettyStatePort,
                                       final boolean noPartialOffloading,
                                       final boolean destroyOffloadingWorker) {
    this.noPartialOffloading = noPartialOffloading;
    this.workerFactory = workerFactory;
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    this.evalConf = evalConf;
    this.workers = new LinkedList<>();
    this.executorId = executorId;
    this.pipeIndexMapWorker = pipeIndexMapWorker;
    this.destroyOffloadingWorker = destroyOffloadingWorker;
    this.offloadingManagerThread = Executors.newCachedThreadPool();

    final OffloadingExecutor offloadingExecutor = new OffloadingExecutor(
      evalConf.offExecutorThreadNum,
      evalConf.samplingJson,
      evalConf.isLocalSource,
      executorId,
      address,
      workerFactory.getDataTransportPort(),
      nettyStatePort);

    final OffloadingExecutorSerializer ser = new OffloadingExecutorSerializer();

    scheduledExecutorService.scheduleAtFixedRate(() -> {
      final StringBuilder sb = new StringBuilder("---------- Lambda byte sent start -------\n");
      synchronized (workers) {
        final List<OffloadingWorker> sorted = new ArrayList<>(workers);
        sorted.sort((worker1, worker2) -> Integer.compare(worker1.getRequestId(), worker2.getRequestId()));

        sorted.forEach(worker -> {
          sb.append("Worker");
          sb.append(worker.getId());
          sb.append(" byteSent ");
          sb.append(worker.getByteSent());
          sb.append(" offloadCnt ");
          sb.append(worker.getNumOffloadedData());
          sb.append("\n");
        });
      }

      sb.append("--------- Lambda byte sent end --------");

      LOG.info(sb.toString());
    }, 1, 1, TimeUnit.SECONDS);

    this.offloadExecutorByteBuf = ByteBufAllocator.DEFAULT.buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(offloadExecutorByteBuf);
    try {
      final ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(offloadingExecutor);
      oos.writeObject(ser.getInputDecoder());
      oos.writeObject(ser.getOutputEncoder());
      oos.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void createWorker(final int num) {
    workers.addAll(createWorkerBlocking(num));
  }

  protected List<OffloadingWorker> createWorkerBlocking(final int num) {
    // workerFactory.createStreamingWorker()
    offloadExecutorByteBuf.retain(num);
    final List<OffloadingWorker> newWorkers = IntStream.range(0, num)
      .boxed().map(i -> {
        final OffloadingWorker worker = workerFactory.createStreamingWorker(
          offloadExecutorByteBuf, new OffloadingExecutorSerializer(), new EventHandler<Pair<OffloadingWorker, OffloadingExecutorControlEvent>>() {
            @Override
            public void onNext(Pair<OffloadingWorker, OffloadingExecutorControlEvent> msg) {
              final Pair<OffloadingWorker, OffloadingExecutorControlEvent> pair = msg;

              final OffloadingExecutorControlEvent oe = pair.right();
              final OffloadingWorker myWorker = pair.left();

              switch (oe.getType()) {
                case TASK_READY: {
                  final ByteBufInputStream bis = new ByteBufInputStream(oe.getByteBuf());
                  try {

                    LOG.info("Task ready readable byte {}", oe.getByteBuf().readableBytes());

                    final String taskId = bis.readUTF();

                    LOG.info("Receive task ready message from worker {} in executor {}: {}",
                      myWorker.getId(),
                      executorId, taskId);

                    final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(taskId);

                    myWorker.addReadyTask(taskId);

                    if (taskReadyBlockingMap.containsKey(taskId)) {
                      final int cnt = taskReadyBlockingMap.get(taskId).decrementAndGet();

                      if (cnt == 0) {
                        taskReadyBlockingMap.remove(taskId);
                        executorThread.addShortcutEvent(new TaskOffloadingEvent(taskId,
                          TaskOffloadingEvent.ControlType.OFFLOAD_DONE,
                          null));
                      }

                    }
                  } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                  }
                  break;
                }
                case TASK_FINISH_DONE: {
                  final ByteBufInputStream bis = new ByteBufInputStream(oe.getByteBuf());
                  try {
                    final String taskId = bis.readUTF();

                    myWorker.removeDoneTask(taskId);

                    synchronized (workers) {
                      LOG.info("Receive task done message in worker {} in executor {}: {}",
                        myWorker.getId(), executorId, taskId);
                      final List<OffloadingWorker> ows = taskWorkerMap.get(taskId);
                      synchronized (ows) {
                        taskWorkerMap.get(taskId).remove(myWorker);
                        if (taskWorkerMap.get(taskId).size() == 0) {
                          // deoffloading done
                          LOG.info("Deoffloading done ....!!!");
                          taskWorkerMap.remove(taskId);
                          taskExecutorMapWrapper.getTaskExecutorThread(taskId).addEvent(new TaskOffloadingEvent(taskId,
                            TaskOffloadingEvent.ControlType.DEOFFLOADING_DONE,
                            null));
                        }
                      }

                      workerTaskMap.get(myWorker).remove(taskId);

                      if (workerTaskMap.get(myWorker).isEmpty()) {
                        // Destroy worker !!
                        if (destroyOffloadingWorker) {
                          LOG.info("Worker destroy {} after deoffloading {} ...", myWorker.getId(), taskId);

                          final ControlMessage.Message message = ControlMessage.Message.newBuilder()
                            .setId(RuntimeIdManager.generateMessageId())
                            .setListenerId(MessageEnvironment.LAMBDA_OFFLOADING_REQUEST_ID)
                            .setType(ControlMessage.MessageType.LambdaEnd)
                            .setLambdaEndMsg(ControlMessage.LambdaEndMessage.newBuilder()
                              .setRequestId(myWorker.getRequestId())
                              .build())
                            .build();

                          myWorker.writeControl(message);

                          workerTaskMap.remove(myWorker);
                          workers.remove(myWorker);
                        }
                      } else {
                        LOG.info("Worker destroy is not empty {} after deoffloadig {} / {} ...", myWorker.getId(),
                          taskId, workerTaskMap.get(myWorker));
                      }

                    }
                  } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                  }
                  break;
                }
                case EXECUTOR_METRICS: {
                  final ByteBufInputStream bis = new ByteBufInputStream(oe.getByteBuf());
                  final DataInputStream dis = new DataInputStream(bis);
                  final ExecutorMetrics executorMetrics = ExecutorMetrics.decode(dis);

                  LOG.info("Executor metrics recieved for worker {}: {}", myWorker.getId(), executorMetrics);
                  myWorker.setMetric(executorMetrics);
                  break;
                }
                default: {
                  throw new RuntimeException("Not supported type " + oe.getType());
                }

              }

              // oe.getByteBuf().release();
            }
          });

        return worker;
      }).collect(Collectors.toList());

    newWorkers.forEach(worker -> {
      while (!worker.isReady()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    LOG.info("Returning new workers for offloading {}", newWorkers.size());

    return newWorkers;
  }

  @Override
  public void deoffloading(String taskId) {
    final Triple<String, String, String> key = pipeIndexMapWorker.getIndexMap().keySet().stream()
      .filter(k -> k.getRight().equals(taskId)).collect(Collectors.toList()).get(0);
    final int pipeIndex = pipeIndexMapWorker.getPipeIndex(key.getLeft(), key.getMiddle(), key.getRight());

    // final Queue<SourceData> d1 = sourceQueueMap.remove(taskId);
    final Queue<TaskHandlingEvent> d2 = intermediateQueueMap.remove(taskId);

    // TODO: flush pending data
    // TODO: fix
    synchronized (taskWorkerMap.get(taskId)) {
      taskWorkerMap.get(taskId).forEach(worker -> {
        worker.writeData
          (pipeIndex,
            new TaskControlMessage(TaskControlMessage.TaskControlMessageType.OFFLOAD_TASK_STOP,
              pipeIndex,
              pipeIndex,
              taskId, null));
      });
    }
  }

  public abstract void createWorkers(final String taskId);
  abstract Optional<List<OffloadingWorker>> selectWorkersForOffloading(final String taskId);

  protected final ExecutorService executorService = Executors.newCachedThreadPool();

  /*
  private synchronized void prepareOffloading(String taskId, ExecutorThreadQueue et) {
    // select worker
    Optional<List<OffloadingWorker>> workers = selectWorkersForOffloading(taskId);

    if (!workers.isPresent()) {
      // blocking call
      executorService.submit(() -> {
        createWorkers(taskId);
        et.addShortcutEvent(new TaskOffloadingEvent(taskId, WORKER_READY, null));
      });
    }
  }
  */

  private final Map<String, AtomicInteger> taskReadyBlockingMap = new ConcurrentHashMap<>();

  protected void offloadTaskToWorker(final String taskId, final List<OffloadingWorker> newWorkers,
                                     final boolean blocking) {
    LOG.info("Offloading task {}, workers: {}", taskId, newWorkers);

    taskReadyBlockingMap.put(taskId, new AtomicInteger(newWorkers.size()));

    newWorkers.forEach(worker -> {
      final ControlMessage.Message message = ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(MessageEnvironment.LAMBDA_OFFLOADING_REQUEST_ID)
        .setType(ControlMessage.MessageType.TaskSendToLambda)
        .setTaskSendToLambdaMsg(ControlMessage.TaskSendToLambdaMessage.newBuilder()
          .setRequestId(worker.getRequestId())
          .setTaskId(taskId)
          .build())
        .build();

      worker.writeControl(message);
    });

    if (blocking) {
      try {
        final AtomicInteger ai = taskReadyBlockingMap.get(taskId);
        while (ai.get() > 0) {
          Thread.sleep(15);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

  }

  @Override
  public void offloading(String taskId) {
    // sourceQueueMap.putIfAbsent(taskId, new ConcurrentLinkedQueue<>());
    intermediateQueueMap.putIfAbsent(taskId, new ConcurrentLinkedQueue<>());

    final Optional<List<OffloadingWorker>> workersForOffloading = selectWorkersForOffloading(taskId);

    if (workersForOffloading.isPresent()) {
      offloadTaskToWorker(taskId, workersForOffloading.get(), false);
    } else {
      // create a new worker for offloading
      // blocking call
      executorService.submit(() -> {
        createWorkers(taskId);
        // et.addShortcutEvent(new TaskOffloadingEvent(taskId, WORKER_READY, null));
        final Optional<List<OffloadingWorker>> newWorkers = selectWorkersForOffloading(taskId);

        if (!newWorkers.isPresent()) {
          throw new RuntimeException("Worker does not present... " + taskId);
        }

        offloadTaskToWorker(taskId, newWorkers.get(), false);
      });
    }


    /*
    offloadingManagerThread.execute(() -> {
      while (intermediateQueueMap.containsKey(taskId)) {
        final AtomicBoolean processed = new AtomicBoolean(false);
        final Queue<TaskHandlingEvent> queue = intermediateQueueMap.get(taskId);

        if (queue != null) {
          while (!queue.isEmpty()) {
            final TaskHandlingEvent pending = queue.peek();
            final Optional<OffloadingWorker> optional =
              selectWorkerForIntermediateOffloading(taskId, pending);

            if (optional.isPresent()) {
              final OffloadingWorker worker = optional.get();
              worker.writeData(pending.getInputPipeIndex(), pending);
              currBufferedData.decrementAndGet();
              processed.set(true);
              queue.poll();
            } else {
              break;
            }
          }
        }

        if (!processed.get()) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }
      }
    });
    */
  }

  private final Map<String, Queue<TaskHandlingEvent>> intermediateQueueMap = new ConcurrentHashMap<>();

  private long totalProcessedDataInWorker(final ExecutorMetrics em) {
    return em.taskInputProcessRateMap.values().stream()
      .map(pair -> pair.right().get())
      .reduce((x, y) -> x + y)
      .get();
  }


  private int PARTIAL_CNT_THRESHOLD = 2000;
  private int PARTIAL_TIME_THRESHOLD = 1000; // ms
  private final AtomicBoolean deactivatePartials = new AtomicBoolean(false);

  @Override
  public boolean offloadPartialDataOrNot(String taskId, TaskHandlingEvent data) {

    // DEACTIVATION CHECK
    if (System.currentTimeMillis() - partialInvokeTime > PARTIAL_TIME_THRESHOLD) {
      if (deactivatePartials.compareAndSet(false, true)) {
        // Deactivation all workers
        LOG.info("Deactivating all workers.. because timed out");
        workers.forEach(worker -> {
          if (worker.isActivated()) {
            worker.deactivate();
          }
          LOG.info("Worker {} offloaded cnt ", worker.getId(), workerParitalOffloadingCntMap.get(worker));
        });
        disablePartialOffloading();
        return false;
      } else {
        disablePartialOffloading();
        return false;
      }
    }

    taskPartialOffloadingCntMap.putIfAbsent(taskId, 0);

    final List<OffloadingWorker> partials = taskPartialOffloadingWorkersMap.get(taskId)
      .stream().filter(worker ->
        workerParitalOffloadingCntMap.get(worker).get() < PARTIAL_CNT_THRESHOLD && worker.isActivated())
      .collect(Collectors.toList());

    if (partials.isEmpty()) {
      return false;
    } else {

      taskPartialOffloadingCntMap.put(taskId, taskPartialOffloadingCntMap.get(taskId) + 1);
      final int index = taskPartialOffloadingCntMap.get(taskId) % partials.size();

      final OffloadingWorker offloadingWorker = partials.get(index);
      final int cnt = workerParitalOffloadingCntMap.get(offloadingWorker).incrementAndGet();

      if (cnt == PARTIAL_CNT_THRESHOLD) {
        offloadingWorker.writeData(data.getInputPipeIndex(), data);
        // deactivation worker!!
        LOG.info("Worker partial offloading cnt is larger than threshold.. deoffloading", offloadingWorker.getId());
        offloadingWorker.deactivate();
        return true;
      } else if (cnt < PARTIAL_CNT_THRESHOLD) {
        // LOG.info("Offloading partial data for task {} to worker {}, cnt {}", taskId, offloadingWorker.getId(), cnt);
        offloadingWorker.writeData(data.getInputPipeIndex(), data);
        return true;
      } else {
        return false;
      }
    }
  }

  private volatile boolean partialOffloadingInvoked = false;

  long partialInvokeTime;

  @Override
  public void invokeParitalOffloading() {
    partialInvokeTime = System.currentTimeMillis();
    deactivatePartials.set(false);
    taskPartialOffloadingCntMap.clear();
    workerParitalOffloadingCntMap.clear();
    workers.forEach(worker -> {
      workerParitalOffloadingCntMap.put(worker, new AtomicInteger(0));
    });

    partialOffloadingInvoked = true;
  }

  private void disablePartialOffloading() {
    invokedBooleanMap.clear();
    partialOffloadingInvoked = false;
  }

  private final Map<String, Boolean> invokedBooleanMap = new ConcurrentHashMap<>();
  private final Map<String, List<OffloadingWorker>> taskPartialOffloadingWorkersMap = new ConcurrentHashMap<>();
  private final Map<OffloadingWorker, AtomicInteger> workerParitalOffloadingCntMap = new ConcurrentHashMap<>();
  private final Map<String, Integer> taskPartialOffloadingCntMap = new ConcurrentHashMap<>();

  @Override
  public boolean canOffloadPartial(String taskId) {
    if (!partialOffloadingInvoked) {
      return false;
    } else {
      if (!invokedBooleanMap.getOrDefault(taskId, false)) {
        if (workers.stream()
          .filter(worker -> worker.hasReadyTask(taskId))
          .allMatch(worker -> worker.isActivated())) {
          invokedBooleanMap.putIfAbsent(taskId, true);
          taskPartialOffloadingWorkersMap.putIfAbsent(taskId,
            workers.stream().filter(worker -> worker.hasReadyTask(taskId)).collect(Collectors.toList()));
          return true;
        } else {
          return false;
        }
      } else {
        return true;
      }
    }
  }

  // private final AtomicInteger c = new AtomicInteger(0);
  @Override
  public void offloadIntermediateData(String taskId, TaskHandlingEvent data) {

    Optional<OffloadingWorker> optional =
      selectWorkerForIntermediateOffloading(taskId, data);

    while (!optional.isPresent()) {
      try {
        Thread.sleep(40);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      optional = selectWorkerForIntermediateOffloading(taskId, data);
    }

    final OffloadingWorker worker = optional.get();

    // LOG.info("Offloading data index {}, cnt {}", data.getInputPipeIndex(), c.getAndIncrement());
    worker.writeData(data.getInputPipeIndex(), data);

    /*
    if (optional.isPresent()) {
      final OffloadingWorker worker = optional.get();
      final Queue<TaskHandlingEvent> queue = intermediateQueueMap.get(taskId);

      if (!worker.isReady()) {
        queue.add(data);
        return;
      }

      if (!queue.isEmpty()) {
        queue.add(data);
        final Iterator<TaskHandlingEvent> iterator = queue.iterator();
        final long prevFlushTime = System.currentTimeMillis();
        long processedData = 0;

        while (iterator.hasNext()
          && System.currentTimeMillis() - prevFlushTime <= 300) {

          final TaskHandlingEvent bufferedData = iterator.next();
          iterator.remove();
          processedData += 1;
          worker.writeData(bufferedData.getInputPipeIndex(), bufferedData);
        }
      } else {
        worker.writeData(data.getInputPipeIndex(), data);
      }

    } else {
      final Queue<TaskHandlingEvent> queue = intermediateQueueMap.get(taskId);
      queue.add(data);
      // throw new RuntimeException("No worker for offloading ... " + taskId);
    }
    */


    /*
    if (noPartialOffloading) {
      final Optional<OffloadingWorker> optional =
        selectWorkerForIntermediateOffloading(taskId, data);

      if (optional.isPresent()) {
        final OffloadingWorker worker = optional.get();
        worker.writeData(data.getInputPipeIndex(), data);
      } else {
        throw new RuntimeException("No worker for offloading ... " + taskId);
      }
    } else {
    */

    /*
    final Queue<TaskHandlingEvent> queue = intermediateQueueMap.get(taskId);
    queue.add(data);
    currBufferedData.incrementAndGet();
    */

    // }

    /*
    while (!queue.isEmpty()) {
      final TaskHandlingEvent pending = queue.peek();
      final Optional<OffloadingWorker> optional =
        selectWorkerForIntermediateOffloading(taskId, pending);

      if (optional.isPresent()) {
        final OffloadingWorker worker = optional.get();
        worker.writeData(pending.getInputPipeIndex(), pending);
        queue.poll();
      } else {
        break;
      }
    }
    */
  }

  abstract Optional<OffloadingWorker> selectWorkerForIntermediateOffloading(String taskId, final TaskHandlingEvent data);
  abstract Optional<OffloadingWorker> selectWorkerForSourceOffloading(String taskId, final Object data);

  // private final Map<String, Queue<SourceData>> sourceQueueMap = new ConcurrentHashMap<>();


  @Override
  public void offloadSourceData(final String taskId,
                                final String edgeId,
                                final Object data,
                                final Serializer serializer) {
    /*
    final int index = pipeIndexMapWorker.getPipeIndex("Origin", edgeId, taskId);

    final Queue<SourceData> sourceQueue = sourceQueueMap.get(taskId);
    sourceQueue.add(new SourceData(index, data, serializer));
    currBufferedData.incrementAndGet();
    */

    /*
    while (!sourceQueue.isEmpty()) {
      final SourceData pending = sourceQueue.peek();
      final Optional<OffloadingWorker> optional = selectWorkerForSourceOffloading(taskId, pending);

      if (optional.isPresent()) {
        final OffloadingWorker worker = optional.get();
        worker.writeSourceData(pending.index, pending.serializer, pending.data);
        sourceQueue.poll();
      } else {
        break;
      }
    }
    */

    // LOG.info("Write source data for offloaded task {}", taskId);
    // workers.get(0).writeSourceData(index, serializer, data);
  }


  @Override
  public void close() {
    isFinished = true;
  }

  final class SourceData {
    public final int index;
    public final Object data;
    public final Serializer serializer;

    SourceData(final int index, final Object data, final Serializer serializer) {
      this.index = index;
      this.data = data;
      this.serializer = serializer;
    }
  }
}
