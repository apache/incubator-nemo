package edu.snu.vortex.runtime.master.resourcemanager;

import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageSender;
import edu.snu.vortex.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.vortex.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.vortex.runtime.exception.NodeConnectionException;
import edu.snu.vortex.runtime.executor.Executor;
import edu.snu.vortex.runtime.executor.ExecutorConfiguration;
import edu.snu.vortex.runtime.executor.block.BlockManagerWorker;
import edu.snu.vortex.runtime.executor.block.LocalStore;
import edu.snu.vortex.runtime.executor.datatransfer.DataTransferFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * {@inheritDoc}
 * Serves as a {@link ResourceManager} in a single machine, local Runtime.
 */
public final class LocalResourceManager implements ResourceManager {
  private final LocalMessageDispatcher localMessageDispatcher;

  public LocalResourceManager(final LocalMessageDispatcher localMessageDispatcher) {
    this.localMessageDispatcher = localMessageDispatcher;
  }

  @Override
  public synchronized Optional<Executor> requestExecutor(final RuntimeAttribute resourceType,
                                                         final ExecutorConfiguration executorConfiguration) {
    final String executorId = RuntimeIdGenerator.generateExecutorId();

    final Map<String, MessageSender<ControlMessage.Message>> nodeIdToMsgSenderMap = new HashMap<>();
    final MessageEnvironment messageEnvironment = new LocalMessageEnvironment(executorId, localMessageDispatcher);
    connectToMaster(nodeIdToMsgSenderMap, messageEnvironment);
    final BlockManagerWorker blockManagerWorker =
        new BlockManagerWorker(executorId, new LocalStore(), messageEnvironment, nodeIdToMsgSenderMap);
    final DataTransferFactory dataTransferFactory = new DataTransferFactory(blockManagerWorker);

    // Create the executor!
    final Executor executor =
        new Executor(executorId,
            executorConfiguration.getDefaultExecutorCapacity(),
            executorConfiguration.getExecutorNumThreads(),
            messageEnvironment,
            nodeIdToMsgSenderMap,
            blockManagerWorker,
            dataTransferFactory);

    return Optional.of(executor);
  }

  /**
   * Initializes connections to master to send necessary messages.
   * @param nodeIdToMsgSenderMap map of node ID to messageSender for outgoing messages from this executor.
   * @param messageEnvironment the message environment for this executor.
   */
  private void connectToMaster(final Map<String, MessageSender<ControlMessage.Message>> nodeIdToMsgSenderMap,
                               final MessageEnvironment messageEnvironment) {
    try {
      nodeIdToMsgSenderMap.put(MessageEnvironment.MASTER_COMMUNICATION_ID,
          messageEnvironment.<ControlMessage.Message>asyncConnect(
              MessageEnvironment.MASTER_COMMUNICATION_ID, MessageEnvironment.MASTER_MESSAGE_RECEIVER).get());
    } catch (Exception e) {
      throw new NodeConnectionException(e);
    }
  }
}
