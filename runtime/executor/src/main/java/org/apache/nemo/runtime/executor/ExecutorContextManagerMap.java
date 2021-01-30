package org.apache.nemo.runtime.executor;

import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransfer;
import org.apache.nemo.runtime.executor.common.datatransfer.ContextManager;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.apache.nemo.runtime.common.message.MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID;

public final class ExecutorContextManagerMap {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorContextManagerMap.class.getName());

  // key: task id, value: executpr od
  private final ConcurrentMap<String, ContextManager>
    executorContextManagerMap = new ConcurrentHashMap<>();

  private final String executorId;
  private final PersistentConnectionToMasterMap toMaster;
  private final ByteTransfer byteTransfer;

  @Inject
  private ExecutorContextManagerMap(
    @Parameter(JobConf.ExecutorId.class) final String executorId,
    final ByteTransfer byteTransfer,
    final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    this.executorId = executorId;
    this.toMaster = persistentConnectionToMasterMap;
    this.byteTransfer = byteTransfer;
  }

  public void init() {
    try {
      final CompletableFuture<ControlMessage.Message> future = toMaster
        .getMessageSender(RUNTIME_MASTER_MESSAGE_LISTENER_ID)
        .request(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(RUNTIME_MASTER_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.CurrentExecutor)
          .build());

      final ControlMessage.Message msg = future.get();
      final List<String> executors = msg.getCurrExecutorsList();
      final ExecutorService es = Executors.newCachedThreadPool();

      initConnectToExecutor(executorId);

      executors.forEach(eid -> {
        if (!eid.equals(executorId)) {
          LOG.info("Initializing executor connection {} -> {}...", executorId, eid);
          es.execute(() -> {
            initConnectToExecutor(eid);
          });
        }
      });

      es.shutdown();
      es.awaitTermination(20, TimeUnit.SECONDS);

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

  }

  public void removeExecutor(final String remoteExecutorId) {
    // TODO: todo
  }

  public synchronized void initConnectToExecutor(final String remoteExecutorId) {
    if (executorContextManagerMap.containsKey(remoteExecutorId)) {
      throw new RuntimeException("Executor " + remoteExecutorId + " already registered");
    }

    LOG.info("Registering  {} -> {}", executorId, remoteExecutorId);

    try {
      executorContextManagerMap.put(remoteExecutorId, byteTransfer.connectTo(remoteExecutorId).get());
      LOG.info("Putting done  {} -> {}", executorId, remoteExecutorId);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public Collection<ContextManager> getExecutorContextManagers() {
    return executorContextManagerMap.values();
  }

  public synchronized ContextManager getExecutorContextManager(final String executorId) {
    // LOG.info("Getting executor context manager {}", executorId);
    return executorContextManagerMap.get(executorId);
  }
}
