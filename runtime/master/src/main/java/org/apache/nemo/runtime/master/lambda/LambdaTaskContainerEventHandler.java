package org.apache.nemo.runtime.master.lambda;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.master.PendingRedirectionTasks;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.apache.reef.tang.InjectionFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.EXECUTOR_MESSAGE_LISTENER_ID;

public final class LambdaTaskContainerEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaTaskContainerEventHandler.class.getName());

  private final ExecutorService singleThread = Executors.newSingleThreadExecutor();
  private final ExecutorRegistry executorRegistry;

  private boolean deactivateInit = false;


  private final Set<String> pendingRedirectionTasks;
  private final InjectionFuture<LambdaContainerManager> lambdaContainerManager;

  @Inject
  private LambdaTaskContainerEventHandler(final ExecutorRegistry executorRegistry,
                                          final PendingRedirectionTasks pendingRedirectionTasks,
                                          final InjectionFuture<LambdaContainerManager> lambdaContainerManager) {
    this.executorRegistry = executorRegistry;
    this.lambdaContainerManager = lambdaContainerManager;
    this.pendingRedirectionTasks = pendingRedirectionTasks.pendingRedirectionTasks;
  }

  public void onAllLambdaTaskScheduled() {
    singleThread.execute(() -> {
      // deactivate tasks

      if (!deactivateInit) {
        deactivateInit = true;
        try {
          Thread.sleep(300);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        executorRegistry.viewExecutors(executors -> {
          executors.forEach(executor -> {
            if (executor.getContainerType().equals(ResourcePriorityProperty.LAMBDA)) {
              LOG.info("Deactivate lambda task for executor {} / {}", executor.getExecutorId(),
                executor.getRunningTasks());
              executor.sendControlMessage(ControlMessage.Message.newBuilder()
                .setId(RuntimeIdManager.generateMessageId())
                .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
                .setType(ControlMessage.MessageType.DeactivateLambdaTask)
                .build());
            }
          });
        });

        LOG.info("Done of task init ... deactivate workers that have no rerouting task");
        // lambdaContainerManager.get().deactivateAllWorkers();
      }
    });
  }
}
