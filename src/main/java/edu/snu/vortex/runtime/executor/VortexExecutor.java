package edu.snu.vortex.runtime.executor;

import edu.snu.vortex.runtime.DataMessageCodec;
import edu.snu.vortex.runtime.TaskGroup;
import edu.snu.vortex.runtime.VortexMessage;
import edu.snu.vortex.runtime.driver.Parameters;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.task.Task;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.task.events.DriverMessage;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.transport.netty.LoggingLinkListener;

import javax.inject.Inject;
import java.util.concurrent.*;

@Unit
public final class VortexExecutor implements Task, TaskMessageSource {

  private final HeartBeatTriggerManager heartBeatTriggerManager;
  private final NetworkConnectionService ncs;
  private final IdentifierFactory idFactory;
  private final BlockManager blockManager;
  private final BlockingDeque<VortexMessage> messagesToDriver;
  private final BlockingDeque<VortexMessage> messagesFromDriver;
  private final String executorId;
  private final int numThreads;

  private final CountDownLatch terminated;

  @Inject
  private VortexExecutor(
      final HeartBeatTriggerManager heartBeatTriggerManager,
      final NetworkConnectionService ncs,
      final IdentifierFactory idFactory,
      @Parameter(TaskConfigurationOptions.Identifier.class)final String executorId,
      @Parameter(Parameters.EvaluatorCore.class) final int numThreads) {
    this.heartBeatTriggerManager = heartBeatTriggerManager;
    this.ncs = ncs;
    this.idFactory = idFactory;
    this.blockManager = new BlockManager(this, ncs, idFactory);
    this.messagesToDriver = new LinkedBlockingDeque<>();
    this.messagesFromDriver = new LinkedBlockingDeque<>();
    this.executorId = executorId;
    this.numThreads = numThreads;

    this.terminated = new CountDownLatch(1);

    ncs.registerConnectionFactory(
        idFactory.getNewInstance(Parameters.NCS_ID),
        new DataMessageCodec(),
        blockManager.getNetworkEventHandler(),
        new LoggingLinkListener<>(),
        idFactory.getNewInstance(executorId));
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    final ExecutorService schedulerThread = Executors.newSingleThreadExecutor();
    final ExecutorService executeThreads = Executors.newFixedThreadPool(numThreads);
    schedulerThread.execute(() -> {
          while (true) {
            // Scheduler Thread: Pick a command to execute (For now, simple FIFO order)
            final VortexMessage message;
            try {
              message = messagesFromDriver.take();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }

            switch (message.getType()) {
              case ExecuteTaskGroup:
                executeThreads.execute(() -> executeTaskGroup((TaskGroup) message.getData()));
                break;
              case ReadRequest:
                blockManager.onReadRequest(message.getExecutorId(), (String) message.getData());
              case ChannelNotReady:
                blockManager.onNotReadyResponse((String) message.getData());
              default:
                throw new RuntimeException("Unknown Command");
            }
          }
        }
    );

    terminated.await();
    return null;
  }

  private void executeTaskGroup(final TaskGroup taskGroup) {
    System.out.println("Executor execute stage: " + taskGroup);
    taskGroup.getTasks().forEach(t -> t.compute());
  }

  void sendReadRequest(final String channelId) {
    final VortexMessage message = new VortexMessage(
        executorId,
        VortexMessage.Type.ReadRequest,
        channelId);
    sendMessage(message);
  }

  void sendChannelReady(String channelId) {
    final VortexMessage message = new VortexMessage(
        executorId,
        VortexMessage.Type.RemoteChannelReady,
        channelId);
    sendMessage(message);
  }

  private void sendMessage(final VortexMessage message) {
    messagesToDriver.addLast(message);
    heartBeatTriggerManager.triggerHeartBeat();
  }

  // Executor to Driver
  @Override
  public Optional<TaskMessage> getMessage() {
    VortexMessage vortexMessage = messagesToDriver.pollFirst();
    if (vortexMessage != null) {
      return Optional.of(TaskMessage.from("", SerializationUtils.serialize(vortexMessage)));
    }

    return Optional.empty();
  }

  // Driver to Executor
  public final class DriverMessageHandler implements EventHandler<DriverMessage> {

    @Override
    public void onNext(final DriverMessage message) {
      if (message.get().isPresent()) {
        final VortexMessage vortexMessage = (VortexMessage) SerializationUtils.deserialize(message.get().get());
        messagesFromDriver.add(vortexMessage);
      }
    }
  }

  public final class TaskCloseHandler implements EventHandler<CloseEvent> {

    @Override
    public void onNext(final CloseEvent closeEvent) {
      ncs.unregisterConnectionFactory(idFactory.getNewInstance(Parameters.NCS_ID));
    }
  }
}
