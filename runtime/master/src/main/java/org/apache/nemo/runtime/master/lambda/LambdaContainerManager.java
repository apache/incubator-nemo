package org.apache.nemo.runtime.master.lambda;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.NettyServerTransport;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.offloading.client.NettyServerSideChannelHandler;
import org.apache.nemo.offloading.client.OffloadingEventHandler;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.common.NettyVMStateStore;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.executor.common.controlmessages.offloading.SendToOffloadingWorker;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingExecutor;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingExecutorSerializer;
import org.apache.nemo.runtime.master.*;
import org.apache.nemo.runtime.master.resource.DefaultExecutorRepresenterImpl;
import org.apache.nemo.runtime.master.resource.ResourceSpecification;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
import org.apache.nemo.runtime.master.scheduler.PairStageTaskManager;
import org.apache.nemo.runtime.message.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty.COMPUTE;
import static org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty.LAMBDA;
import static org.apache.nemo.runtime.executor.common.OffloadingExecutorEventType.EventType.TASK_START;
import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.EXECUTOR_MESSAGE_LISTENER_ID;
import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.LAMBDA_OFFLOADING_REQUEST_ID;


public final class LambdaContainerManager {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaContainerManager.class.getName());

  private final NettyServerTransport workerControlTransport;
  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private final OffloadingEventHandler nemoEventHandler;
  private final ConcurrentMap<Channel, EventHandler<OffloadingMasterEvent>> channelEventHandlerMap;

  private final ExecutorService channelThread;
  private volatile boolean finished = false;
  private final EvalConf evalConf;
  private final ExecutorRegistry executorRegistry;
  private final SerializedTaskMap serializedTaskMap;
  private final PipeIndexMaster pipeIndexMaster;
  private final LambdaContainerRequester requester;
  private final ExecutorService initService = Executors.newCachedThreadPool();
  private final Map<Integer, ByteBuf> requestWorkerInitMap = new ConcurrentHashMap<>();
  private final AtomicInteger requestIdCnt = new AtomicInteger();
  private final Map<Integer, WorkerControlProxy> requestIdControlChannelMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, EventHandler<OffloadingMasterEvent>> requestIdHandlerMap;
  private final Map<Integer, String> requestIdExecutorMap = new ConcurrentHashMap<>();
  private final Map<Integer, LambdaContainerRequester.LambdaActivator> requestIdActivatorMap = new ConcurrentHashMap<>();

  private final TaskScheduledMapMaster taskScheduledMapMaster;

  private final AtomicInteger numRequestedLambda = new AtomicInteger(0);
  private final Set<WorkerControlProxy> pendingActivationWorkers = new HashSet<>();

  private final NemoNameServer nameServer;

  private final NettyVMStateStore stateStore;

  private final Set<Integer> lambdaWorkerInitDoneSet = new HashSet<>();

  private final MessageEnvironment messageEnvironment;

  private final ExecutorService serializationExecutorService; // Executor service for scheduling message serialization.

  private final PairStageTaskManager pairStageTaskManager;

  private final ScheduledExecutorService scheduledExecutorService;

  private final LocalAddressProvider localAddressProvider;

  private final String optPolicy;

  private final ClientRPC clientRPC;

  @Inject
  private LambdaContainerManager(@Parameter(JobConf.ScheduleSerThread.class) final int scheduleSerThread,
                                 @Parameter(JobConf.OptimizationPolicy.class) final String optPolicy,
                                 final TcpPortProvider tcpPortProvider,
                                 final EvalConf evalConf,
                                 final ExecutorRegistry executorRegistry,
                                 final ClientRPC clientRPC,
                                 final PipeIndexMaster pipeIndexMaster,
                                 final SerializedTaskMap serializedTaskMap,
                                 final PairStageTaskManager pairStageTaskManager,
                                 final TaskScheduledMapMaster taskScheduledMapMaster,
                                 final LambdaContainerRequester requester,
                                 final MessageEnvironment messageEnvironment,
                                 final NemoNameServer nameServer,
                                 final NettyVMStateStore stateStore,
                                 @Parameter(EvalConf.Ec2.class) final boolean ec2,
                                 final LocalAddressProvider localAddressProvider) {
    this.clientRPC = clientRPC;
    this.optPolicy = optPolicy;
    this.nameServer = nameServer;
    this.stateStore = stateStore;
    this.evalConf = evalConf;
    this.pairStageTaskManager = pairStageTaskManager;
    this.taskScheduledMapMaster = taskScheduledMapMaster;
    this.serializedTaskMap = serializedTaskMap;
    this.executorRegistry = executorRegistry;
    this.pipeIndexMaster = pipeIndexMaster;
    this.requester = requester;
    this.localAddressProvider = localAddressProvider;
    this.requestIdHandlerMap = new ConcurrentHashMap<>();

    this.serializationExecutorService = Executors.newFixedThreadPool(scheduleSerThread);

    this.scheduledExecutorService = Executors.newScheduledThreadPool(10);

    this.messageEnvironment = messageEnvironment;
    this.channelThread = Executors.newSingleThreadExecutor();
    this.channelEventHandlerMap = new ConcurrentHashMap<>();
    this.nemoEventHandler = new OffloadingEventHandler(channelEventHandlerMap, requestIdHandlerMap);
    this.workerControlTransport = new NettyServerTransport(
      tcpPortProvider, new NettyChannelInitializer(
      new NettyServerSideChannelHandler(serverChannelGroup, nemoEventHandler)),
      new NioEventLoopGroup(30,
        new DefaultThreadFactory("WorkerControlTransport")),
      ec2);

    messageEnvironment
      .setupListener(LAMBDA_OFFLOADING_REQUEST_ID,
        new MessageReceiver());

    LOG.info("Started offloading worker manager...");

    channelThread.execute(() -> {
      while (!finished) {
        try {
          final Pair<Integer, Pair<Channel, OffloadingMasterEvent>> event =
            nemoEventHandler.getHandshakeQueue().take();

          initService.execute(() -> {
            final int requestId = event.left();
            final Pair<Channel, OffloadingMasterEvent> pair = event.right();

            LOG.info("Channel for requestId {}: {}", requestId, pair.left());
            while (!requestIdActivatorMap.containsKey(requestId)) {
              try {
                Thread.sleep(50);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }

            final WorkerControlProxy proxy = new WorkerControlProxy(
              requestId, requestIdExecutorMap.get(requestId), pair.left(),
              clientRPC,
              requestIdActivatorMap.get(requestId), pendingActivationWorkers);

            requestIdHandlerMap.put(requestId, proxy);
            requestIdControlChannelMap.put(requestId, proxy);
            channelEventHandlerMap.put(pair.left(), proxy);
          });

          initService.execute(() -> {
            final Pair<Channel, OffloadingMasterEvent> workerDonePair;
            try {
              workerDonePair = nemoEventHandler.getWorkerReadyQueue().take();
            } catch (InterruptedException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }

            final int port = workerDonePair.right().getByteBuf().readInt();
            final int rid = workerDonePair.right().getByteBuf().readInt();

            workerDonePair.right().getByteBuf().release();
            final String addr = workerDonePair.left().remoteAddress().toString().split(":")[0];

            LOG.info("Worker init done for {} / {}",
              requestIdExecutorMap.get(rid), rid);

            synchronized (lambdaWorkerInitDoneSet) {
              lambdaWorkerInitDoneSet.add(rid);
            }
          });

        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
  }

  private final ExecutorService executorService = Executors.newCachedThreadPool();

  public Future redirectionToLambda(final Collection<String> lambdaTasks,
                                  final ExecutorRepresenter lambdaExecutor) {
    if (lambdaTasks.isEmpty()) {
      return CompletableFuture.completedFuture(0);
    }

    return executorService.submit(() -> {
      // redirection signal to the origin task
      lambdaTasks.forEach(lambdaTaskId -> {
          final String vmTaskId = pairStageTaskManager.getPairTaskEdgeId(lambdaTaskId).get(0).left();
          final String vmExecutorId = taskScheduledMapMaster.getTaskExecutorIdMap().get(vmTaskId);
        try {
          final ExecutorRepresenter vmExecutor = executorRegistry.getExecutorRepresentor(vmExecutorId);
          lambdaExecutor.activateLambdaTask(lambdaTaskId, vmTaskId, vmExecutor);
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException("Exception for activating vmTask " + vmTaskId + ", vmExecutorId: "
            + vmExecutorId + ", lambdaTask: " + lambdaTaskId);
        }

//        lambdaExecutor.getRunningTasks().stream()
//          .map(t -> t.getTaskId())
//          .noneMatch(tid -> tid.equals(lambdaTaskId))
//          &&

        while (!lambdaExecutor.isActivated(lambdaTaskId)) {
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      });
    });
  }


  public void redirectionDoneLambda(final Collection<String> lambdaTasks,
                                    final ExecutorRepresenter lambdaExecutor) {
    if (lambdaTasks.isEmpty()) {
      return;
    }

    executorService.execute(() -> {
      if (!lambdaExecutor.getLambdaControlProxy().isActive()) {
        throw new RuntimeException("Lambda " + lambdaExecutor.getExecutorId() +
          " is inactive ... but try to redirect done " + lambdaTasks);
      }

      lambdaTasks.forEach(lambdaTask -> {
        lambdaExecutor.deactivateLambdaTask(lambdaTask);
      });
    });
  }

  public boolean isAllWorkerActive() {
    return requestIdControlChannelMap.size() == numRequestedLambda.get()
     && requestIdControlChannelMap.values().stream()
      .allMatch(worker -> worker.isActive());
  }

  public void warmup() {
    LOG.info("Activating all workers...");
    final List<WorkerControlProxy> pending = new LinkedList<>();

    requestIdControlChannelMap.values().forEach(worker -> {
      if (!worker.isActive()) {
        pending.add(worker);
        LOG.info("Activating worker {}", worker.getId());
        worker.activate();
      }
    });

    long st = System.currentTimeMillis();

    while (!pending.isEmpty()) {
      final Iterator<WorkerControlProxy> iter = pending.iterator();
      while (iter.hasNext()) {
        final WorkerControlProxy worker = iter.next();
        if (worker.isActive()) {
          worker.deactivate();
          iter.remove();
        }
      }

      if (System.currentTimeMillis() - st >= 10000) {
        LOG.warn("Worker is not activated now {}", pending);
        st = System.currentTimeMillis();
      }

      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private final AtomicBoolean activatedLock = new AtomicBoolean(false);

  public void activateAllWorkers() {
    LOG.info("Activating all workers...");
    synchronized (pendingActivationWorkers) {
      activatedLock.set(true);
      if (!pendingActivationWorkers.isEmpty()) {
        throw new RuntimeException("Still pending activation workers " + pendingActivationWorkers);
      }
      requestIdControlChannelMap.values().forEach(worker -> {
        if (!worker.isActive()) {
          pendingActivationWorkers.add(worker);
          initService.execute(() -> {
            LOG.info("Activating worker {}", worker.getId());
            worker.activate();
          });
        }
      });
    }

    LOG.info("Waiting for activation of all workers {}", pendingActivationWorkers.size());
    long prevLog = System.currentTimeMillis();
    while (!pendingActivationWorkers.isEmpty()) {
      if (System.currentTimeMillis() - prevLog >= 1000) {
        LOG.info("Waiting for activation of all workers {}", pendingActivationWorkers);
        prevLog = System.currentTimeMillis();
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void releaseActivateLock() {
    synchronized (pendingActivationWorkers) {
      activatedLock.set(false);
    }
  }

  public void deactivateNoActivateTaskWorkers() {
    LOG.info("Deactivating no activate task workers...");
    requestIdControlChannelMap.values().forEach(worker -> {
      if (worker.getExecutorRepresenter().getNumOfActivatedAndPendingTasks() == 0
        && worker.isActive())
        worker.deactivate();
    });
  }

  public void destroy() {
    requestIdControlChannelMap.values().forEach(worker -> {
      if (worker.isActive()) {
        worker.deactivate();
      }
    });
    requestIdControlChannelMap.values().forEach(worker -> {
      while (!worker.isDeactivated()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
  }

  public void deactivateAllWorkers() {
    LOG.info("Deactivating all workers...");

    synchronized (pendingActivationWorkers) {
      if (activatedLock.get()) {
        // skip
        LOG.info("Activated lock.. deactivate skip...");
      } else {
        LOG.info("Starting deactivate workers...");
        requestIdControlChannelMap.values().forEach(worker -> {
          if (!worker.isActive()) {
            LOG.info("Worker inactive " + worker.getId() + ", deactivated "
              + worker.isDeactivated() + ", activating " + worker.isActivating());
          }

          ((DefaultExecutorRepresenterImpl) worker.getExecutorRepresenter()).checkAndDeactivate();

          // worker.deactivate();
        });
      }
    }
  }

  public int numLambdaContainer() {
    return requestIdControlChannelMap.size();
  }


  public void close() {
    finished = true;
    channelThread.shutdownNow();
  }

  public int getPort() {
    return this.workerControlTransport.getPort();
  }

  public void stopLambdaContainer(final int num) {
    requestIdControlChannelMap.values().forEach(workerProxy -> {
      LOG.info("Deactivating lambda " + workerProxy.getId() + "/" + workerProxy.getExecutorId());
      workerProxy.deactivate();
    });
  }

  public List<Future<ExecutorRepresenter>> createLambdaContainer(final int num,
                                                                 final int capacity,
                                                                 final int slot,
                                                                 final int memory,
                                                                 final String resourceType) {
    final List<Future<ExecutorRepresenter>> list = new ArrayList<>(num);

    for (int i = 0; i < num; i++) {
      LOG.info("Number of requested resource {}: {}", resourceType, num);

      numRequestedLambda.getAndIncrement();

      list.add(initService.submit(() -> {

        LOG.info("Creating lamdba request {}", resourceType);
        final LambdaContainerRequester.LambdaActivator activator =
          requester.createRequest(workerControlTransport.getLocalAddress(),
            workerControlTransport.getPort(),
            resourceType, capacity, slot, memory);

        final int rid = activator.getRequestId();
        final String lambdaExecutorId = activator.getExecutorId();
        LOG.info("Requested lambda executor {}, resource type {}", lambdaExecutorId, resourceType);

        requestIdExecutorMap.put(rid, lambdaExecutorId);


        requestIdActivatorMap.put(rid, activator);

        final OffloadingExecutor offloadingExecutor = new OffloadingExecutor(
          evalConf.offExecutorThreadNum,
          evalConf.samplingJson,
          evalConf.isLocalSource,
          stateStore.getPort(),
          localAddressProvider.getLocalAddress(),
          nameServer.getPort(),
          lambdaExecutorId,
          evalConf.flushPeriod,
          evalConf.controlLogging,
          evalConf.latencyLimit,
          evalConf.ec2,
          evalConf.optimizationPolicy,
          resourceType);


        final OffloadingExecutorSerializer ser = new OffloadingExecutorSerializer();

        final ByteBuf offloadExecutorByteBuf = ByteBufAllocator.DEFAULT.buffer();
        final ByteBufOutputStream bos = new ByteBufOutputStream(offloadExecutorByteBuf);
        final DataOutputStream dos = new DataOutputStream(bos);
        offloadingExecutor.encode(dos);
        //  SerializationUtils.serialize(ser.getInputDecoder(), dos);
        //  SerializationUtils.serialize(ser.getOutputEncoder(), dos);
        try {
          dos.close();
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }


        long st = System.currentTimeMillis();
        while (!requestIdControlChannelMap.containsKey(rid)) {
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

          if (System.currentTimeMillis() - st >= 100000) {
            throw new RuntimeException("Cannot create lambda container " + lambdaExecutorId);
          }
        }

        final WorkerControlProxy proxy = requestIdControlChannelMap.get(rid);
        LOG.info("Waiting worker init for {}.. send buffer {}", rid, offloadExecutorByteBuf.readableBytes());
        proxy.getControlChannel()
          .writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.WORKER_INIT, offloadExecutorByteBuf));

        // Waiting worker init
        st = System.currentTimeMillis();
        while (true) {
          if (!lambdaWorkerInitDoneSet.contains(rid)) {
            try {
              Thread.sleep(50);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            if (System.currentTimeMillis() - st >= 30000) {
              throw new RuntimeException("Cannot init lambda container for " + rid + "/ " + lambdaExecutorId);
            }
          } else {
            break;
          }
        }

        LOG.info("End of worker init {}", rid);

        // Connect to the executor and initiate Master side's executor representation.
        MessageSender messageSender;
        try {
          messageSender =
            messageEnvironment.asyncConnect(lambdaExecutorId, EXECUTOR_MESSAGE_LISTENER_ID).get();
        } catch (final InterruptedException | ExecutionException e) {
          // TODO #140: Properly classify and handle each RPC failure
          messageSender = new FailedMessageSender();
          throw new RuntimeException("Failed message sender " + lambdaExecutorId);
        }

        // TODO: fix memory and slot
        final ResourceSpecification lambdaResourceSpec =
          new ResourceSpecification(resourceType, capacity, slot, memory);

        final ExecutorRepresenter er = new DefaultExecutorRepresenterImpl(lambdaExecutorId,
          lambdaResourceSpec,
          messageSender,
          () -> {proxy.deactivate();},
          serializationExecutorService,
          lambdaExecutorId,
          serializedTaskMap,
          optPolicy);

        // proxy.setRepresentor(er);
        er.setLambdaControlProxy(proxy);
        // DEACTIVATE IMMEDIATELY !!!

        if (resourceType.equals(LAMBDA)) {
          proxy.deactivate();

          if (evalConf.partialWarmup) {
            LOG.info("Setup partial warmup");
            scheduledExecutorService.schedule(() -> {
              partialWarmup(rid, er);
            }, evalConf.partialWarmupPeriod, TimeUnit.SECONDS);
          }
        }

        return er;
      }));
    }

    return list;
  }

  private void partialWarmup(final int rid,
                             final ExecutorRepresenter er) {
    if (requestIdControlChannelMap.containsKey(rid)) {
      LOG.info("Start to partial warmup haha {}", evalConf.optimizationPolicy);
      if (evalConf.optimizationPolicy.contains("R2") || evalConf.optimizationPolicy.contains("R3")) {
        LOG.info("Start to partial warmup haha 222 {}", evalConf.optimizationPolicy);
        try {
          er.partialWarmupStatelessTasks(1.0, taskScheduledMapMaster, executorRegistry, pairStageTaskManager);
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException("Cannot trigger partial warmup");
        }
        scheduledExecutorService.schedule(() -> {
          LOG.info("Trigger partial warmup");
          partialWarmup(rid, er);
        }, evalConf.partialWarmupPeriod, TimeUnit.SECONDS);
      }
    }
  }


  public final class MessageReceiver implements MessageListener<ControlMessage.Message> {

    private final ExecutorService executorService = Executors.newFixedThreadPool(50);
    private final Map<String, ByteBuf> offloadExecutorByteBufMap = new ConcurrentHashMap<>();

    @Override
    public void onMessage(ControlMessage.Message message) {
      throw new RuntimeException("Not support " + message);
    }

    @Override
    public void onMessageWithContext(ControlMessage.Message message, MessageContext messageContext) {
      throw new RuntimeException("Not support " + message);
    }
  }
}
