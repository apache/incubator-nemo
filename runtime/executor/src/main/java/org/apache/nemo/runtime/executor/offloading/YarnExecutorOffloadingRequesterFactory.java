package org.apache.nemo.runtime.executor.offloading;

import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.offloading.client.OffloadingEventHandler;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

public final class YarnExecutorOffloadingRequesterFactory implements OffloadingRequesterFactory {

  private final double cpulimit;
  private final MessageEnvironment messageEnvironment;
  private final PersistentConnectionToMasterMap toMasterMap;
  private final String executorId;

  @Inject
  private YarnExecutorOffloadingRequesterFactory(
    final MessageEnvironment messageEnvironment,
    final PersistentConnectionToMasterMap toMasterMap,
    @Parameter(JobConf.ExecutorId.class) final String executorId,
    @Parameter(EvalConf.OffloadingCpuLimit.class) final double cpulimit) {
    this.messageEnvironment = messageEnvironment;
    this.toMasterMap = toMasterMap;
    this.executorId = executorId;
    this.cpulimit = cpulimit;
  }

  @Override
  public OffloadingRequester getInstance(OffloadingEventHandler eventHandler, String address, int port) {
    return new YarnExecutorOffloadingRequester(address, port,
      messageEnvironment, toMasterMap, executorId);
  }
}
