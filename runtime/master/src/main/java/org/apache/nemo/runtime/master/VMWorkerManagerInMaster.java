package org.apache.nemo.runtime.master;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.common.NettyServerTransport;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.TransferKey;
import org.apache.nemo.common.VMWorkerConf;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.client.*;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.NettyChannelInitializer;
import org.apache.nemo.offloading.common.OffloadingMasterEvent;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class VMWorkerManagerInMaster {
  private static final Logger LOG = LoggerFactory.getLogger(VMWorkerManagerInMaster.class.getName());

  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private OffloadingEventHandler nemoEventHandler;
  private final ConcurrentMap<Channel, EventHandler<OffloadingMasterEvent>> channelEventHandlerMap;

  private final NettyServerTransport nettyServerTransport;

  private final AtomicBoolean initialized = new AtomicBoolean(false);

  private final VMOffloadingRequester requestor;
  private final Map<String, String> taskExecutorIdMap;
  private final Map<String, Pair<String, Integer>> executorAddressMap;
  private final Map<TransferKey, Integer> taskTransferIndexMap;
  private final VMWorkerConf vmWorkerConf;
  private final Map<String, Pair<Double, Double>> executorCpuUseMap;

  @Inject
  private VMWorkerManagerInMaster(
    final TcpPortProvider tcpPortProvider,
    final RendevousServer rendevousServer,
    final NameServer nameServer,
    final LocalAddressProvider localAddressProvider,
    final EvalConf evalConf,
    final TaskScheduledMapMaster taskScheduledMap,
    final TransferIndexMaster transferIndexMaster,
    final ExecutorCpuUseMap cpuMap) {
    this.channelEventHandlerMap = new ConcurrentHashMap<>();
    this.nemoEventHandler = new OffloadingEventHandler(channelEventHandlerMap);
    this.nettyServerTransport = new NettyServerTransport(
      tcpPortProvider, new NettyChannelInitializer(
      new NettyServerSideChannelHandler(serverChannelGroup, nemoEventHandler)),
      new NioEventLoopGroup(2,
        new DefaultThreadFactory("VMWorkerManager")),
      true);

    this.executorCpuUseMap = cpuMap.getExecutorCpuUseMap();

    LOG.info("Netty server lambda transport created end");
    initialized.set(true);

    this.taskExecutorIdMap = taskScheduledMap.getTaskExecutorIdMap();
    this.executorAddressMap = taskScheduledMap.getExecutorAddressMap();
    this.taskTransferIndexMap = transferIndexMaster.transferIndexMap;

    this.vmWorkerConf = new VMWorkerConf(evalConf.executorThreadNum,
      executorAddressMap,
      transferIndexMaster.serializerMap,
      taskExecutorIdMap,
      taskTransferIndexMap,
      rendevousServer.getPublicAddress(),
      rendevousServer.getPort(),
      localAddressProvider.getLocalAddress(),
      nameServer.getPort());

    this.requestor = new VMOffloadingRequester(
      nemoEventHandler, nettyServerTransport.getLocalAddress(), nettyServerTransport.getPort(),
      vmWorkerConf, evalConf, executorCpuUseMap);
  }

  public List<CompletableFuture<VMScalingWorker>> createWorkers(final int num) {
    return requestor.createWorkers(num);
  }
}
