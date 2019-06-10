package org.apache.nemo.runtime.executor;

import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class StageOffloadingWorkerManager {

  private static final Logger LOG = LoggerFactory.getLogger(StageOffloadingWorkerManager.class.getName());

  private final Map<String, Boolean> offloadingMap;
  private final Map<String, Long> offloadingStartTimeMap;
  private final Map<String, AtomicInteger> stageOffloadingCntMap;
  private final PersistentConnectionToMasterMap toMaster;
  private final String executorId;

  private final long cachePeriod = TimeUnit.SECONDS.toMillis(20);

  @Inject
  private StageOffloadingWorkerManager(
    @Parameter(JobConf.ExecutorId.class) String executorId,
    final PersistentConnectionToMasterMap toMaster) {

    this.executorId = executorId;
    this.offloadingMap = new HashMap<>();
    this.stageOffloadingCntMap = new HashMap<>();
    this.offloadingStartTimeMap = new HashMap<>();
    this.toMaster = toMaster;
  }

  private void increaseCnt(final String stageId) {
    final AtomicInteger count =
      stageOffloadingCntMap.getOrDefault(stageId, new AtomicInteger(0));

    count.getAndIncrement();
    stageOffloadingCntMap.putIfAbsent(stageId, count);
  }

  public boolean isStageOffloadable(final String stageId) {

    if (offloadingStartTimeMap.containsKey(stageId)) {
      final long time = offloadingStartTimeMap.get(stageId);
      final long currtime = System.currentTimeMillis();
      if (currtime - time >= cachePeriod) {
        // invalidate
        offloadingMap.remove(stageId);
        stageOffloadingCntMap.remove(stageId);
        offloadingStartTimeMap.remove(stageId);
      }
    }


    if (offloadingMap.containsKey(stageId)) {
      if (offloadingMap.get(stageId)) {
        increaseCnt(stageId);
      }
      return offloadingMap.get(stageId);
    }

    final CompletableFuture<ControlMessage.Message> msgFuture = toMaster
      .getMessageSender(MessageEnvironment.STAGE_OFFLOADING_LISTENER_ID).request( ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.STAGE_OFFLOADING_LISTENER_ID)
          .setType(ControlMessage.MessageType.RequestStageOffloading)
          .setRequestStageOffloadingMsg(ControlMessage.RequestStageOffloadingMessage.newBuilder()
            .setExecutorId(executorId)
            .setStageId(stageId)
            .build())
          .build());

    try {
      final ControlMessage.Message msg = msgFuture.get();

      LOG.info("Can offloading {}: {}", stageId, msg.getStageOffloadingInfoMsg().getCanOffloading());
      offloadingMap.put(stageId, msg.getStageOffloadingInfoMsg().getCanOffloading());

      if (offloadingMap.get(stageId)) {
        increaseCnt(stageId);
      }

      offloadingStartTimeMap.put(stageId, System.currentTimeMillis());

      return offloadingMap.get(stageId);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void endOffloading(final String stageId) {
    final AtomicInteger count =
      stageOffloadingCntMap.get(stageId);

    LOG.info("Offloading cnt {} of {}", count, stageId);

    if (count.decrementAndGet() == 0) {
      LOG.info("Offloading done for stage {}", stageId);
      sendOffloadingDoneEvent(stageId);
    }
  }

  private void sendOffloadingDoneEvent(final String stageId) {
    LOG.info("Send offloading done for {}", stageId);
    final CompletableFuture<ControlMessage.Message> msgFuture = toMaster
      .getMessageSender(MessageEnvironment.STAGE_OFFLOADING_LISTENER_ID).request(
        ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.STAGE_OFFLOADING_LISTENER_ID)
          .setType(ControlMessage.MessageType.RequestStageOffloadingDone)
          .setRequestStageOffloadingDoneMsg(ControlMessage.RequestStageOffloadingDoneMessage.newBuilder()
            .setExecutorId(executorId)
            .setStageId(stageId)
            .build())
          .build());
  }

}
