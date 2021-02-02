package org.apache.nemo.runtime.executor.offloading;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.executor.NettyStateStore;
import org.apache.nemo.runtime.executor.PipeIndexMapWorker;
import org.apache.nemo.runtime.executor.TaskExecutorMapWrapper;
import org.apache.nemo.runtime.executor.TinyTaskWorker;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransport;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.controlmessages.offloading.SendToOffloadingWorker;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingExecutor;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingExecutorSerializer;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.nemo.runtime.executor.common.OffloadingExecutorEventType.EventType.TASK_START;


public final class OffloadingManagerImpl implements OffloadingManager {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingManagerImpl.class.getName());

  // private final List<OffloadingWorker> workers;
  private final List<OffloadingWorker> workers;
  private final OffloadingWorkerFactory workerFactory;
  private final ConcurrentMap<String, TaskExecutor> offloadedTaskMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, TinyTaskWorker> deletePendingWorkers = new ConcurrentHashMap<>();

  private final TaskExecutorMapWrapper taskExecutorMapWrapper;
  private final EvalConf evalConf;

  private final ByteTransport byteTransport;
  private final String executorId;
  private final NettyStateStore nettyStateStore;
  private final PipeIndexMapWorker pipeIndexMapWorker;

  @Inject
  private OffloadingManagerImpl(final OffloadingWorkerFactory workerFactory,
                                final TaskExecutorMapWrapper taskExecutorMapWrapper,
                                final EvalConf evalConf,
                                final PipeIndexMapWorker pipeIndexMapWorker,
                                final NettyStateStore nettyStateStore,
                                @Parameter(JobConf.ExecutorId.class) final String executorId,
                                final ByteTransport byteTransport) {
    this.workerFactory = workerFactory;
    this.taskExecutorMapWrapper = taskExecutorMapWrapper;
    this.evalConf = evalConf;
    this.workers = new LinkedList<>();
    this.executorId = executorId;
    this.byteTransport = byteTransport;
    this.nettyStateStore = nettyStateStore;
    this.pipeIndexMapWorker = pipeIndexMapWorker;
  }

  @Override
  public void createWorker(final int num) {
    // workerFactory.createStreamingWorker()
    final OffloadingExecutor offloadingExecutor = new OffloadingExecutor(
      evalConf.executorThreadNum,
      evalConf.samplingJson,
      evalConf.isLocalSource,
      executorId,
      byteTransport.getPublicAddress(),
      workerFactory.getDataTransportPort(),
      nettyStateStore.getPort());

    final OffloadingExecutorSerializer ser = new OffloadingExecutorSerializer();

    final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);
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

    final OffloadingWorker worker = workerFactory.createStreamingWorker(
      byteBuf, new OffloadingExecutorSerializer(), new EventHandler() {
        @Override
        public void onNext(Object msg) {
          final OffloadingEvent oe = (OffloadingEvent) msg;
          if (oe.getType().equals(OffloadingEvent.Type.TASK_READY)) {
            final ByteBufInputStream bis = new ByteBufInputStream(oe.getByteBuf());
            try {
              final String taskId = bis.readUTF();
              final ExecutorThread executorThread = taskExecutorMapWrapper.getTaskExecutorThread(taskId);
              LOG.info("Receive task ready message from offloading worker in executor {}: {}", executorId, taskId);

              executorThread.addEvent(new TaskOffloadingEvent(taskId,
                TaskOffloadingEvent.ControlType.OFFLOAD_DONE,
                null));
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }
      });

    workers.add(worker);
  }

  @Override
  public void offloading(String taskId) {
    LOG.info("Offloading task {}, indexMap: {}", taskId, pipeIndexMapWorker.getIndexMap());
    final byte[] bytes = taskExecutorMapWrapper.getTaskSerializedByte(taskId);
    final SendToOffloadingWorker taskSend =
      new SendToOffloadingWorker(bytes, pipeIndexMapWorker.getIndexMap());
    final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);

    try {
      bos.writeUTF(taskId);
      bos.writeInt(TASK_START.ordinal());
      taskSend.encode(bos);
      bos.close();
      workers.get(0).writeControl(new OffloadingEvent(OffloadingEvent.Type.TASK_SEND, byteBuf));
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeData(String taskId, TaskHandlingEvent data) {
    workers.get(0).writeData(data.getInputPipeIndex(), data.getDataByteBuf());
  }
}
