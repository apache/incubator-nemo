package edu.snu.vortex.runtime.master.resourcemanager;

import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageSender;
import edu.snu.vortex.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.vortex.runtime.executor.Executor;
import edu.snu.vortex.runtime.master.BlockManagerMaster;
import edu.snu.vortex.runtime.master.scheduler.Scheduler;

import java.util.HashMap;
import java.util.Map;

/**
 * {@inheritDoc}
 * Serves as a {@link ResourceManager} in a single machine, local Runtime.
 */
public final class LocalResourceManager implements ResourceManager {

  private final Scheduler scheduler;

  /**
   * Map of executor IDs to the actual executors.
   */
  private final Map<String, Executor> executorMap;
  private final MessageEnvironment masterMessageEnvironment;
  private final LocalMessageDispatcher localMessageDispatcher;
  private final BlockManagerMaster blockManagerMaster;

  public LocalResourceManager(final Scheduler scheduler,
                              final MessageEnvironment masterMessageEnvironment,
                              final LocalMessageDispatcher localMessageDispatcher,
                              final BlockManagerMaster blockManagerMaster) {
    this.scheduler = scheduler;
    this.masterMessageEnvironment = masterMessageEnvironment;
    this.localMessageDispatcher = localMessageDispatcher;
    this.blockManagerMaster = blockManagerMaster;
    this.executorMap = new HashMap<>();
  }

  @Override
  public synchronized void requestExecutor(final RuntimeAttribute resourceType, final int executorCapacity) {
    final String executorId = RuntimeIdGenerator.generateExecutorId();

    // Create the executor!
    final Executor executor =
        new Executor(executorId, executorCapacity, localMessageDispatcher, blockManagerMaster);
    executorMap.put(executorId, executor);

    // Connect to the executor and initiate Master side's executor representation.
    final MessageSender messageSender;
    try {
      messageSender =
          masterMessageEnvironment.asyncConnect(executorId, MessageEnvironment.EXECUTOR_MESSAGE_RECEIVER).get();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    final ExecutorRepresenter executorRepresenter =
        new ExecutorRepresenter(executorId, resourceType, executorCapacity, messageSender);
    scheduler.onExecutorAdded(executorRepresenter);
  }
}
