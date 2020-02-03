package org.apache.nemo.runtime.executor.burstypolicy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.Channel;
import org.apache.beam.sdk.io.kafka.KafkaUnboundedSource;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.coder.FSTSingleton;
import org.apache.nemo.compiler.frontend.beam.source.UnboundedSourceReadable;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.common.OffloadingEvent;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.executor.task.DefaultTaskExecutorImpl;
import org.apache.nemo.runtime.executor.task.TinyTaskOffloader;
import org.apache.nemo.runtime.executor.vmscaling.VMScalingWorkerConnector;
import org.apache.nemo.runtime.lambdaexecutor.StateOutput;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingTask;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class VMScalingFromSfToVmHandler {
  private static final Logger LOG = LoggerFactory.getLogger(VMScalingFromSfToVmHandler.class.getName());

  private final List<Wrapper> moveTaskList;
  private final ExecutorService executorService;
  private final EvalConf evalConf;

  @Inject
  private VMScalingFromSfToVmHandler(
    final VMScalingWorkerConnector vmScalingWorkerConnector,
    final EvalConf evalConf) {
    this.moveTaskList = new LinkedList<>();
    this.executorService = Executors.newFixedThreadPool(10);
    this.evalConf = evalConf;
  }

  public void registerForTaskMove(
    final Object m,
    final TaskExecutor offloadedTask,
    final String newExecutorId,
    final Channel workerChannel) {

    synchronized (moveTaskList) {
      moveTaskList.add(new Wrapper(m, offloadedTask, newExecutorId, workerChannel));
    }
  }

  public void moveToVMScaling() {
    LOG.info("Start to moving tasks to vm scaling workers");

    for (final Wrapper wrapper : moveTaskList) {
      // do sth
      final TaskExecutor offloadedTask = wrapper.offloadedTask;
      final Object m = wrapper.m;
      final String newExecutorId = wrapper.newExecutorId;
      final Channel workerChannel = wrapper.channel;

      LOG.info("Ready to move task {} to {}", offloadedTask.getId(),
        newExecutorId);

      // move task to vm!!
      // 1. offloading task
      final TinyTaskOffloader offloader =
        (TinyTaskOffloader) ((DefaultTaskExecutorImpl) offloadedTask).offloader.get();

      final OffloadingTask offloadingTask = new OffloadingTask(
        newExecutorId,
        offloader.taskId,
        RuntimeIdManager.getIndexFromTaskId(offloader.taskId),
        evalConf.samplingJson,
        offloader.copyDag,
        offloader.taskOutgoingEdges,
        offloader.copyOutgoingEdges,
        offloader.copyIncomingEdges);

      LOG.info("Sending offloading task {} to {}", offloader.taskId, newExecutorId);

      executorService.execute(() -> {
        workerChannel.writeAndFlush(
          new OffloadingEvent(OffloadingEvent.Type.OFFLOADING_TASK, offloadingTask.encode()));

        // 2. ready task
        if (m instanceof StateOutput) {

          final StateOutput stateOutput = (StateOutput) m;

          LOG.info("Sending middle task {} to {}, readable {}",
            offloader.taskId, newExecutorId, stateOutput.byteBuf.readableBytes());

          workerChannel.writeAndFlush(
            new OffloadingEvent(OffloadingEvent.Type.MIDDLE_TASK, stateOutput.byteBuf));

        } else if (m instanceof KafkaOffloadingOutput) {
          final KafkaOffloadingOutput output = (KafkaOffloadingOutput) m;

          final long prev = offloader.sourceVertexDataFetcher.getPrevWatermarkTimestamp();
          final UnboundedSourceReadable readable = (UnboundedSourceReadable)
            offloader.sourceVertexDataFetcher.getReadable();
          final KafkaUnboundedSource unboundedSource =
            (KafkaUnboundedSource) readable.getUnboundedSource();

          try {
            final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
            final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);
            bos.writeLong(prev);
            final FSTConfiguration conf = FSTSingleton.getInstance();
            conf.encodeToStream(bos, unboundedSource);

            final CompositeByteBuf compositeByteBuf =
              ByteBufAllocator.DEFAULT.compositeBuffer(2)
                .addComponents(true, byteBuf, output.byteBuf);

            workerChannel.writeAndFlush(
              new OffloadingEvent(OffloadingEvent.Type.SOURCE_TASK, compositeByteBuf));
          } catch (final Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }

          LOG.info("Sending source task {} to {}", offloader.taskId, newExecutorId);

        } else {
          throw new RuntimeException("Unsuported type " + m);
        }
      });
    }
  }

  final class Wrapper {
    public final Object m;
    public final String newExecutorId;
    public final TaskExecutor offloadedTask;
    public final Channel channel;

    public Wrapper(final Object m,
                   final TaskExecutor offloadedTask,
                   final String newExecutorId,
                   final Channel channel) {
      this.m = m;
      this.newExecutorId = newExecutorId;
      this.offloadedTask = offloadedTask;
      this.channel = channel;
    }
  }
}
