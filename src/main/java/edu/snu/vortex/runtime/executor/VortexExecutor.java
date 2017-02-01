package edu.snu.vortex.runtime.executor;

import edu.snu.vortex.compiler.backend.vortex.SourceTask;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
public final class VortexExecutor implements Task, TaskMessageSource {

  private final Logger LOG = Logger.getLogger(VortexExecutor.class.getName());

  private final HeartBeatTriggerManager heartBeatTriggerManager;
  private final NetworkConnectionService ncs;
  private final IdentifierFactory idFactory;
  private final BlockManager blockManager;
  private final BlockingDeque<VortexMessage> messagesToDriver;
  private final BlockingDeque<VortexMessage> messagesFromDriver;
  private final String executorId;
  private final int numThreads;

  private final CountDownLatch terminated;

  private final Map<String, TaskGroup> cachedTaskGroup;

  @Inject
  private VortexExecutor(
      final HeartBeatTriggerManager heartBeatTriggerManager,
      final NetworkConnectionService ncs,
      final IdentifierFactory idFactory,
      @Parameter(TaskConfigurationOptions.Identifier.class)final String executorId,
      @Parameter(Parameters.EvaluatorCore.class) final int numThreads) {
    LOG.log(Level.INFO, "Executor id = {0}", executorId);
    this.heartBeatTriggerManager = heartBeatTriggerManager;
    this.ncs = ncs;
    this.idFactory = idFactory;
    this.blockManager = new BlockManager(this, ncs, idFactory);
    this.messagesToDriver = new LinkedBlockingDeque<>();
    this.messagesFromDriver = new LinkedBlockingDeque<>();
    this.executorId = executorId;
    this.numThreads = numThreads;

    this.terminated = new CountDownLatch(1);
    this.cachedTaskGroup = new HashMap<>();

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
    final ExecutorService resubmitThread = Executors.newSingleThreadExecutor();
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
                final TaskGroup taskGroup = (TaskGroup) message.getData();
                cachedTaskGroup.put(taskGroup.getId(), taskGroup);
                final boolean unboundedSource = taskGroup.getTasks().stream()
                    .filter(task -> task instanceof SourceTask)
                    .anyMatch(sourceTask -> ((SourceTask)sourceTask).isUnbounded());
                if (unboundedSource) {
                  System.out.println("Unbounded schedule");
                  executeThreads.execute(() -> {
                    executeTaskGroup(taskGroup);
                    resubmitThread.execute(() -> {
                      try {
                        Thread.sleep(5 * 1000);
                        messagesFromDriver.add(message);
                      } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                      }
                    });
                  });
                  /*
                  executeThreads.execute(() -> {
                    while (true) {
                      try {
                        executeTaskGroup(taskGroup);
                        final Object object = new Object();
                        synchronized (object) {
                          Thread.sleep(10 * 1000);
                        }
                      } catch (Exception e) {
                        throw new RuntimeException(e);
                      }
                    }
                  });
                  */
                }
                else {
                  System.out.println("Bounded schedule");
                  executeThreads.execute(() -> executeTaskGroup(taskGroup));
                }
                break;
              case ExecutedCachedTaskGroup:
                final TaskGroup cached = cachedTaskGroup.get((String)message.getData());
                executeTaskGroup(cached);
                break;
              case ReadRequest:
                blockManager.onReadRequest(message.getTargetExecutorId(), (String) message.getData());
                break;
              case ChannelNotReady:
                blockManager.onNotReadyResponse((String) message.getData());
                break;
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
