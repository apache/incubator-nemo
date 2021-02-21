package org.apache.nemo.runtime.executor.offloading;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.coder.FSTSingleton;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.executor.PipeIndexMapWorker;
import org.apache.nemo.runtime.executor.TaskExecutorMapWrapper;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.executor.common.controlmessages.offloading.SendToOffloadingWorker;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingExecutor;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingExecutorSerializer;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final PipeIndexMapWorker pipeIndexMapWorker;
  private final ByteBuf offloadExecutorByteBuf;

  private final ExecutorService offloadingManagerThread;
  private volatile boolean isFinished = false;

  protected final AtomicLong currBufferedData = new AtomicLong(0);

  private boolean noPartialOffloading;

  private final boolean destroyOffloadingWorker;

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
          offloadExecutorByteBuf, new OffloadingExecutorSerializer(), new EventHandler() {
            @Override
            public void onNext(Object msg) {
              final Pair<OffloadingWorker, OffloadingEvent> pair = (Pair<OffloadingWorker, OffloadingEvent>) msg;
              final OffloadingEvent oe = pair.right();
              final OffloadingWorker myWorker = pair.left();

              switch (oe.getType()) {
                case TASK_READY: {
                  final ByteBufInputStream bis = new ByteBufInputStream(oe.getByteBuf());
                  try {
                    final String taskId = bis.readUTF();
                    final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(taskId);
                    LOG.info("Receive task ready message from worker {} in executor {}: {}",
                      myWorker.getId(),
                      executorId, taskId);

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
                          LOG.info("Worker destroy {} ...", myWorker.getId());
                          myWorker.writeControl(new OffloadingEvent(OffloadingEvent.Type.END, null));
                          workerTaskMap.remove(myWorker);
                          workers.remove(myWorker);
                        }
                      }

                    }
                    LOG.info("Receive task done message from prepareOffloading worker in executor {}: {}", executorId, taskId);
                  } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                  }
                  break;
                }
                case EXECUTOR_METRICS: {
                  final ByteBufInputStream bis = new ByteBufInputStream(oe.getByteBuf());
                  final ExecutorMetrics executorMetrics;
                  try {
                    executorMetrics = (ExecutorMetrics) FSTSingleton.getInstance().decodeFromStream(bis);
                  } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                  }
                  LOG.info("Executor metrics recieved for worker {}: {}", myWorker.getId(), executorMetrics);
                  myWorker.setMetric(executorMetrics);
                  break;
                }
                default: {
                  throw new RuntimeException("Not supported type " + oe.getType());
                }

              }

              oe.getByteBuf().release();
            }
          });

        return worker;
      }).collect(Collectors.toList());

    newWorkers.forEach(worker -> {
      while (!worker.isReady()) {
        try {
          Thread.sleep(10);
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
    final byte[] bytes = taskExecutorMapWrapper.getTaskSerializedByte(taskId);
    final SendToOffloadingWorker taskSend =
      new SendToOffloadingWorker(taskId, bytes, pipeIndexMapWorker.getIndexMap(), true);
    final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);

    try {
      bos.writeUTF(taskId);
      bos.writeInt(TASK_START.ordinal());
      taskSend.encode(bos);
      bos.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    taskReadyBlockingMap.put(taskId, new AtomicInteger(newWorkers.size()));

    byteBuf.retain(newWorkers.size());

    newWorkers.forEach(worker -> {
      worker.writeControl(new OffloadingEvent(OffloadingEvent.Type.TASK_SEND, byteBuf));
    });

    byteBuf.release();

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

  @Override
  public void offloadIntermediateData(String taskId, TaskHandlingEvent data) {

    final Optional<OffloadingWorker> optional =
      selectWorkerForIntermediateOffloading(taskId, data);

    if (optional.isPresent()) {
      final OffloadingWorker worker = optional.get();
      worker.writeData(data.getInputPipeIndex(), data);
    } else {
      throw new RuntimeException("No worker for offloading ... " + taskId);
    }

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
