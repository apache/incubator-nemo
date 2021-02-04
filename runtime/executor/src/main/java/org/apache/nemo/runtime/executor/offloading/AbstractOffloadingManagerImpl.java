package org.apache.nemo.runtime.executor.offloading;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Triple;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.nemo.runtime.executor.common.OffloadingExecutorEventType.EventType.TASK_START;
import static org.apache.nemo.runtime.executor.common.TaskOffloadingEvent.ControlType.WORKER_READY;


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

  public AbstractOffloadingManagerImpl(final OffloadingWorkerFactory workerFactory,
                                       final TaskExecutorMapWrapper taskExecutorMapWrapper,
                                       final EvalConf evalConf,
                                       final PipeIndexMapWorker pipeIndexMapWorker,
                                       final String executorId,
                                       final String address,
                                       final int nettyStatePort) {
    this.workerFactory = workerFactory;
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    this.evalConf = evalConf;
    this.workers = new LinkedList<>();
    this.executorId = executorId;
    this.pipeIndexMapWorker = pipeIndexMapWorker;


    final OffloadingExecutor offloadingExecutor = new OffloadingExecutor(
      evalConf.executorThreadNum,
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

  protected List<OffloadingWorker> createWorkerBlocking(final int num) {
    // workerFactory.createStreamingWorker()
    offloadExecutorByteBuf.retain();
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
                    LOG.info("Receive task ready message from prepareOffloading worker in executor {}: {}", executorId, taskId);

                    executorThread.addShortcutEvent(new TaskOffloadingEvent(taskId,
                      TaskOffloadingEvent.ControlType.OFFLOAD_DONE,
                      null));
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
                    synchronized (workers) {
                      taskWorkerMap.get(taskId).remove(myWorker);
                      workerTaskMap.get(myWorker).remove(taskId);

                      if (workerTaskMap.get(myWorker).isEmpty()) {
                        // Destroy worker !!
                        LOG.info("Worker destroy...");
                        myWorker.writeControl(new OffloadingEvent(OffloadingEvent.Type.END, null));
                        workerTaskMap.remove(myWorker);
                        workers.remove(myWorker);
                      }

                      if (taskWorkerMap.get(taskId).size() == 0) {
                        taskWorkerMap.remove(taskId);
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
                  final ExecutorMetrics executorMetrics = SerializationUtils.deserialize(bis);
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
          Thread.sleep(300);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    return newWorkers;
  }

  @Override
  public void deoffloading(String taskId) {
    final Triple<String, String, String> key = pipeIndexMapWorker.getIndexMap().keySet().stream()
      .filter(k -> k.getRight().equals(taskId)).collect(Collectors.toList()).get(0);
    final int pipeIndex = pipeIndexMapWorker.getPipeIndex(key.getLeft(), key.getMiddle(), key.getRight());

    taskWorkerMap.get(taskId).forEach(worker -> {
      worker.writeData
        (pipeIndex,
          new TaskControlMessage(TaskControlMessage.TaskControlMessageType.OFFLOAD_TASK_STOP,
            pipeIndex,
            pipeIndex,
            taskId, null));
    });
  }

  abstract void createWorkers(final String taskId);
  abstract Optional<List<OffloadingWorker>> selectWorkersForOffloading(final String taskId);

  private final ExecutorService executorService = Executors.newFixedThreadPool(30);

  @Override
  public synchronized void prepareOffloading(String taskId, ExecutorThreadQueue et) {
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

  @Override
  public void offloading(String taskId) {
    final Optional<List<OffloadingWorker>> workers = selectWorkersForOffloading(taskId);

    if (!workers.isPresent()) {
      throw new RuntimeException("Worker does not present... " + taskId);
    }

    LOG.info("Offloading task {}, indexMap: {}", taskId, pipeIndexMapWorker.getIndexMap());
    final byte[] bytes = taskExecutorMapWrapper.getTaskSerializedByte(taskId);
    final SendToOffloadingWorker taskSend =
      new SendToOffloadingWorker(bytes, pipeIndexMapWorker.getIndexMap(), true);
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

    byteBuf.retain();
    workers.get().forEach(worker -> {
      worker.writeControl(new OffloadingEvent(OffloadingEvent.Type.TASK_SEND, byteBuf));
    });

    byteBuf.release();
  }

  @Override
  public void offloadIntermediateData(String taskId, TaskHandlingEvent data) {
    final OffloadingWorker offloadingWorker =
    selectWorkerForIntermediateOffloading(taskId, data);
    offloadingWorker.writeData(data.getInputPipeIndex(), data);
  }

  abstract OffloadingWorker selectWorkerForIntermediateOffloading(String taskId, final TaskHandlingEvent data);
  abstract OffloadingWorker selectWorkerForSourceOffloading(String taskId, final Object data);

  @Override
  public void offloadSourceData(final String taskId,
                                final String edgeId,
                                final Object data,
                                final Serializer serializer) {
    final int index = pipeIndexMapWorker.getPipeIndex("Origin", edgeId, taskId);
    final OffloadingWorker worker = selectWorkerForSourceOffloading(taskId, data);
    worker.writeSourceData(index, serializer, data);

    // LOG.info("Write source data for offloaded task {}", taskId);
    // workers.get(0).writeSourceData(index, serializer, data);
  }
}
