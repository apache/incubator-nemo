package edu.snu.vortex.runtime.evaluator;

import edu.snu.vortex.runtime.VortexMessage;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.task.Task;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.task.events.DriverMessage;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

@Unit
public final class VortexExecutor implements Task, TaskMessageSource {

  private final HeartBeatTriggerManager heartBeatTriggerManager;

  private final BlockingDeque<VortexMessage> messagesToDriver;

  @Inject
  private VortexExecutor(final HeartBeatTriggerManager heartBeatTriggerManager) {
    this.heartBeatTriggerManager = heartBeatTriggerManager;
    this.messagesToDriver = new LinkedBlockingDeque<>();
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {

    return null;
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

    }
  }

  public final class TaskCloseHandler implements EventHandler<CloseEvent> {

    @Override
    public void onNext(final CloseEvent closeEvent) {

    }
  }
}
