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
import org.apache.nemo.common.ResourceSpecBuilder;
import org.apache.nemo.common.RuntimeIdManager;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty.COMPUTE;
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
  private final Map<Integer, String> requestIdExecutorMap = new ConcurrentHashMap<>();

  private final TaskScheduledMapMaster taskScheduledMapMaster;

  private final AtomicInteger numRequestedLambda = new AtomicInteger(0);
  private final Set<WorkerControlProxy> pendingActivationWorkers = new HashSet<>();

  private final NemoNameServer nameServer;
  private final LocalAddressProvider localAddressProvider;

  private final NettyVMStateStore stateStore;

  private final Set<Integer> lambdaWorkerInitDoneSet = new HashSet<>();

  private final MessageEnvironment messageEnvironment;

  private final ExecutorService serializationExecutorService; // Executor service for scheduling message serialization.

  @Inject
  private LambdaContainerManager(@Parameter(JobConf.ScheduleSerThread.class) final int scheduleSerThread,
                                 final TcpPortProvider tcpPortProvider,
                                 final EvalConf evalConf,
                                 final ExecutorRegistry executorRegistry,
                                 final PipeIndexMaster pipeIndexMaster,
                                 final SerializedTaskMap serializedTaskMap,
                                 final TaskScheduledMapMaster taskScheduledMapMaster,
                                 final LambdaContainerRequester requester,
                                 final MessageEnvironment messageEnvironment,
                                 final NemoNameServer nameServer,
                                 final NettyVMStateStore stateStore,
                                 final LocalAddressProvider localAddressProvider) {
    this.nameServer = nameServer;
    this.stateStore = stateStore;
    this.localAddressProvider = localAddressProvider;
    this.evalConf = evalConf;
    this.taskScheduledMapMaster = taskScheduledMapMaster;
    this.serializedTaskMap = serializedTaskMap;
    this.executorRegistry = executorRegistry;
    this.pipeIndexMaster = pipeIndexMaster;
    this.requester = requester;

    this.serializationExecutorService = Executors.newFixedThreadPool(scheduleSerThread);

    this.messageEnvironment = messageEnvironment;
    this.channelThread = Executors.newSingleThreadExecutor();
    this.channelEventHandlerMap = new ConcurrentHashMap<>();
    this.nemoEventHandler = new OffloadingEventHandler(channelEventHandlerMap);
    this.workerControlTransport = new NettyServerTransport(
      tcpPortProvider, new NettyChannelInitializer(
      new NettyServerSideChannelHandler(serverChannelGroup, nemoEventHandler)),
      new NioEventLoopGroup(30,
        new DefaultThreadFactory("WorkerControlTransport")),
      false);

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
            final WorkerControlProxy proxy = new WorkerControlProxy(
              requestId, requestIdExecutorMap.get(requestId), pair.left(), pendingActivationWorkers);

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

  public void close() {
    finished = true;
    channelThread.shutdownNow();
  }

  public String getAddress() {
    return this.workerControlTransport.getPublicAddress();
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

  public List<Future<ExecutorRepresenter>> createLambdaContainer(final int num) {

    final List<Future<ExecutorRepresenter>> list = new ArrayList<>(num);

    for (int i = 0; i < num; i++) {

      list.add(initService.submit(() -> {
        final int rid = requestIdCnt.getAndIncrement();
        final String lambdaExecutorId = "Lambda-" + rid;
        LOG.info("Request lambda executor " + lambdaExecutorId);

        requestIdExecutorMap.put(rid, lambdaExecutorId);

        requester.createRequest(workerControlTransport.getPublicAddress(),
          workerControlTransport.getPort(), rid, "Lambda-" + rid);

        final OffloadingExecutor offloadingExecutor = new OffloadingExecutor(
          evalConf.executorThreadNum,
          evalConf.samplingJson,
          evalConf.isLocalSource,
          stateStore.getPort(),
          localAddressProvider.getLocalAddress(),
          nameServer.getPort(),
          lambdaExecutorId,
          evalConf.flushPeriod,
          evalConf.controlLogging,
          evalConf.latencyLimit);


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

          if (System.currentTimeMillis() - st >= 60000) {
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
          new ResourceSpecification(COMPUTE, 1, 100, 1024);

        final ExecutorRepresenter er = new DefaultExecutorRepresenterImpl(lambdaExecutorId,
          lambdaResourceSpec,
          messageSender,
          () -> {proxy.deactivate();},
          serializationExecutorService,
          lambdaExecutorId,
          serializedTaskMap);

        proxy.setRepresentor(er);

        return er;
      }));
    }

    return list;
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