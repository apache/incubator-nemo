package org.apache.nemo.runtime.master;

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
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.client.NettyServerSideChannelHandler;
import org.apache.nemo.offloading.client.NettyServerTransport;
import org.apache.nemo.offloading.client.OffloadingEventHandler;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.executor.common.controlmessages.offloading.SendToOffloadingWorker;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingExecutor;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingExecutorSerializer;
import org.apache.nemo.runtime.master.offloading.OffloadingRequester;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.nemo.runtime.master.scheduler.ExecutorRegistry;
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

import static org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty.SOURCE;
import static org.apache.nemo.runtime.executor.common.OffloadingExecutorEventType.EventType.TASK_START;


public final class OffloadingWorkerManager {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingWorkerManager.class.getName());

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
  private final OffloadingRequester offloadingRequester;
  private final ExecutorService initService = Executors.newCachedThreadPool();
  private final Map<Integer, ByteBuf> requestWorkerInitMap = new ConcurrentHashMap<>();
  private final AtomicInteger requestIdCnt = new AtomicInteger();
  private final Map<Integer, WorkerControlProxy> requestIdControlChannelMap = new ConcurrentHashMap<>();
  private final Map<Integer, String> requestIdExecutorMap = new ConcurrentHashMap<>();

  private final TaskScheduledMapMaster taskScheduledMapMaster;

  private final AtomicInteger numRequestedLambda = new AtomicInteger(0);
  private final Set<WorkerControlProxy> pendingActivationWorkers = new HashSet<>();

  @Inject
  private OffloadingWorkerManager(final TcpPortProvider tcpPortProvider,
                                  final EvalConf evalConf,
                                  final ExecutorRegistry executorRegistry,
                                  final PipeIndexMaster pipeIndexMaster,
                                  final SerializedTaskMap serializedTaskMap,
                                  final TaskScheduledMapMaster taskScheduledMapMaster,
                                  final OffloadingRequester offloadingRequester,
                                  final MessageEnvironment messageEnvironment) {
    this.evalConf = evalConf;
    this.taskScheduledMapMaster = taskScheduledMapMaster;
    this.serializedTaskMap = serializedTaskMap;
    this.executorRegistry = executorRegistry;
    this.pipeIndexMaster = pipeIndexMaster;
    this.offloadingRequester = offloadingRequester;
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
      .setupListener(MessageEnvironment.LAMBDA_OFFLOADING_REQUEST_ID,
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
            final ByteBuf workerInitBuffer = requestWorkerInitMap.remove(requestId);

            if (workerInitBuffer == null) {
              throw new RuntimeException("No worker init buffer for request id " + requestId);
            }

            LOG.info("Channel for requestId {}: {}", requestId, pair.left());
            final WorkerControlProxy proxy = new WorkerControlProxy(
              requestId, requestIdExecutorMap.get(requestId), pair.left(), pendingActivationWorkers);

            requestIdControlChannelMap.put(requestId, proxy);
            channelEventHandlerMap.put(pair.left(), proxy);

            LOG.info("Waiting worker init.. send buffer {}", workerInitBuffer.readableBytes());
            pair.left().writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.WORKER_INIT, workerInitBuffer));
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

            LOG.info("Send data channel address to executor {}: {}/{}:{}",
              requestIdExecutorMap.get(rid),
              rid, addr, port);

            final String fullAddr = addr + ":" + port;

            final ExecutorRepresenter er =
              executorRegistry.getExecutorRepresentor(requestIdExecutorMap.get(rid));

            er.sendControlMessage(ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.LAMBDA_OFFLOADING_REQUEST_ID)
              .setType(ControlMessage.MessageType.LambdaControlChannel)
              .setGetLambaControlChannelMsg(ControlMessage.GetLambdaControlChannel.
                newBuilder()
                .setFullAddr(fullAddr)
                .setRequestId(rid)
                .build())
              .build());

            // activate !!
            requestIdControlChannelMap.get(rid).setDataChannel(er, fullAddr);

            if (evalConf.partialWarmup && isAllWorkerActive()) {
              // DEACTIVATE !!
              deactivateAllWorkers();
            }
          });


        } catch (InterruptedException e) {
          e.printStackTrace();
        }


      }
    });

  }

  public void activateAllWorkers() {
    LOG.info("Activating all workers...");
    synchronized (pendingActivationWorkers) {
      if (!pendingActivationWorkers.isEmpty()) {
        throw new RuntimeException("Still pending activation workers " + pendingActivationWorkers);
      }

      pendingActivationWorkers.addAll(requestIdControlChannelMap.values());

      requestIdControlChannelMap.values().forEach(worker -> {
        if (!worker.isActive()) {
          initService.execute(() -> {
            LOG.info("Activating worker {}", worker.getId());
            offloadingRequester.createChannelRequest(workerControlTransport.getPublicAddress(),
              workerControlTransport.getPort(), worker.getId(), worker.getExecutorId());
          });
        }
      });
    }
  }

  public void deactivateAllWorkers() {
    LOG.info("Deactivating all workers...");

    while (!isAllWorkerActive()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    LOG.info("Starting deactivate workers...");

    requestIdControlChannelMap.values().forEach(worker -> {
      if (!worker.isActive()) {
        throw new RuntimeException("Worker still active " + worker.getId());
      }

      worker.deactivate();
    });
  }

  public boolean isAllWorkerActive() {
    return requestIdControlChannelMap.size() == numRequestedLambda.get()
     && requestIdControlChannelMap.values().stream()
      .allMatch(worker -> worker.isActive());
  }

  public void sendTaskToLambda() {

    activateAllWorkers();

    requestIdControlChannelMap.values().forEach(proxy -> {
      initService.execute(() -> {

        while (!proxy.isActive()) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        if (!proxy.isActive()) {
          throw new RuntimeException("Worker " + proxy.getId() + " is not active.. cannot send task");
        }

        taskScheduledMapMaster.getTaskExecutorIdMap().forEach((taskId, executorId) -> {
          if (executorId.equals(proxy.getExecutorId())) {

            final SendToOffloadingWorker taskSend =
              new SendToOffloadingWorker(taskId,
                serializedTaskMap.getSerializedTask(taskId),
                pipeIndexMaster.getIndexMapForTask(taskId), true);
            final ByteBuf byteBuf = proxy.getControlChannel().alloc().ioBuffer(50);
            final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);

            try {
              bos.writeUTF(taskId);
              bos.writeInt(TASK_START.ordinal());
              taskSend.encode(bos);
              bos.close();
            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }

            LOG.info("Task send {} to lambda {}", taskId, proxy);
            proxy.sendTask(taskId, (new OffloadingMasterEvent(
              OffloadingMasterEvent.Type.TASK_SEND, bos.buffer())));
          }
        });

        LOG.info("Waiting for worker {} ready all task", proxy.getId());

        while (!(pendingActivationWorkers.isEmpty() && proxy.allPendingTasksReady())) {
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        LOG.info("Worker {} pending task is ready ... try to deactivate", proxy.getId());
        proxy.deactivate();
      });
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



  public final class MessageReceiver implements MessageListener<ControlMessage.Message> {

    private final ExecutorService executorService = Executors.newFixedThreadPool(50);
    private final Map<String, ByteBuf> offloadExecutorByteBufMap = new ConcurrentHashMap<>();

    @Override
    public void onMessage(ControlMessage.Message message) {
      switch (message.getType()) {
        // for warmup offloading
        case ExecutorPreparedForLambda: {

          if (evalConf.partialWarmup) {
            final ControlMessage.LambdaCreateMessage m = message.getLambdaCreateMsg();
            final int numLambda = evalConf.numLambdaPool;

            if (!offloadExecutorByteBufMap.containsKey(m.getExecutorId())) {
              final OffloadingExecutor offloadingExecutor = new OffloadingExecutor(
                evalConf.offExecutorThreadNum,
                evalConf.samplingJson,
                evalConf.isLocalSource,
                m.getExecutorId(),
                m.getDataChannelAddr(),
                (int) m.getDataChannelPort(),
                (int) m.getNettyStatePort());

              final OffloadingExecutorSerializer ser = new OffloadingExecutorSerializer();

              final ByteBuf offloadExecutorByteBuf = ByteBufAllocator.DEFAULT.buffer();
              final ByteBufOutputStream bos = new ByteBufOutputStream(offloadExecutorByteBuf);
              final DataOutputStream dos = new DataOutputStream(bos);
              offloadingExecutor.encode(dos);
              SerializationUtils.serialize(ser.getInputDecoder(), dos);
              SerializationUtils.serialize(ser.getOutputEncoder(), dos);
              try {
                dos.close();
              } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
              }

              offloadExecutorByteBufMap.putIfAbsent(m.getExecutorId(), offloadExecutorByteBuf);

            }

            LOG.info("Receive lambda create request {}, {}, {}, {}, {}",
              m.getExecutorId(), m.getDataChannelAddr(), m.getDataChannelPort(), m.getNettyStatePort(), numLambda);

            final ByteBuf offloadExecutorByteBuf = offloadExecutorByteBufMap.get(m.getExecutorId());
            offloadExecutorByteBuf.retain(numLambda);

            numRequestedLambda.getAndAdd(numLambda);

            for (int i = 0; i < numLambda; i++) {
              final int rid = requestIdCnt.getAndIncrement();
              requestIdExecutorMap.put(rid, m.getExecutorId());
              requestWorkerInitMap.put(rid, offloadExecutorByteBuf);

              executorService.execute(() -> {
                offloadingRequester.createChannelRequest(workerControlTransport.getPublicAddress(),
                  workerControlTransport.getPort(), rid, m.getExecutorId());
              });
            }

            executorRegistry.getExecutorRepresentor(m.getExecutorId())
              .sendControlMessage(ControlMessage.Message.newBuilder()
                .setId(RuntimeIdManager.generateMessageId())
                .setListenerId(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID)
                .setType(ControlMessage.MessageType.CreateOffloadingExecutor)
                .setSetNum(numLambda)
                .build());
          }
          break;
        }
        // for normal offloading
        case LambdaEnd: {
          final int requestId = (int) message.getLambdaEndMsg().getRequestId();
          final Channel channel = requestIdControlChannelMap.get(requestId).getControlChannel();
          LOG.info("Lambda End of {}/{}", requestId, channel.remoteAddress());
          channel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.END, null));
          break;
        }
        // for normal offloading
        case TaskSendToLambda: {

          if (evalConf.partialWarmup) {
            LOG.info("Partial warm-up enabled... prevent TaskSendToLambda");
            return;
          }

          final ControlMessage.TaskSendToLambdaMessage m = message.getTaskSendToLambdaMsg();

          executorService.execute(() -> {

            final int requestId = (int) m.getRequestId();
            final WorkerControlProxy proxy = requestIdControlChannelMap.get(requestId);

            while (!proxy.isActive()) {
              try {
                Thread.sleep(10);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }

            if (proxy.isActive()) {
              final SendToOffloadingWorker taskSend =
                new SendToOffloadingWorker(m.getTaskId(),
                  serializedTaskMap.getSerializedTask(m.getTaskId()),
                  pipeIndexMaster.getIndexMapForTask(m.getTaskId()), true);
              final ByteBuf byteBuf = proxy.getControlChannel().alloc().ioBuffer();
              final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);

              try {
                bos.writeUTF(m.getTaskId());
                bos.writeInt(TASK_START.ordinal());
                taskSend.encode(bos);
                bos.close();
              } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
              }

              LOG.info("Task send {} to lambda {}", m.getTaskId(), requestId);

              proxy.sendTask(m.getTaskId(), new OffloadingMasterEvent(
                OffloadingMasterEvent.Type.TASK_SEND, bos.buffer()));

            }
          });

          break;
        }
        // for normal offloading
        case LambdaCreate: {

          if (evalConf.partialWarmup) {
            LOG.info("Partial warm-up enabled... prevent LambdaCreate");
            return;
          }

          final ControlMessage.LambdaCreateMessage m = message.getLambdaCreateMsg();
          final int numLambda = (int) m.getNumberOfLambda();

          if (!offloadExecutorByteBufMap.containsKey(m.getExecutorId())) {
            final OffloadingExecutor offloadingExecutor = new OffloadingExecutor(
              evalConf.offExecutorThreadNum,
              evalConf.samplingJson,
              evalConf.isLocalSource,
              m.getExecutorId(),
              m.getDataChannelAddr(),
              (int) m.getDataChannelPort(),
              (int) m.getNettyStatePort());

            final OffloadingExecutorSerializer ser = new OffloadingExecutorSerializer();

            final ByteBuf offloadExecutorByteBuf = ByteBufAllocator.DEFAULT.buffer();
            final ByteBufOutputStream bos = new ByteBufOutputStream(offloadExecutorByteBuf);
            final DataOutputStream dos = new DataOutputStream(bos);
            offloadingExecutor.encode(dos);
            SerializationUtils.serialize(ser.getInputDecoder(), dos);
            SerializationUtils.serialize(ser.getOutputEncoder(), dos);
            try {
              dos.close();
            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }

            offloadExecutorByteBufMap.putIfAbsent(m.getExecutorId(), offloadExecutorByteBuf);

          }

          LOG.info("Receive lambda create request {}, {}, {}, {}, {}",
            m.getExecutorId(), m.getDataChannelAddr(), m.getDataChannelPort(), m.getNettyStatePort(), numLambda);

          final ByteBuf offloadExecutorByteBuf = offloadExecutorByteBufMap.get(m.getExecutorId());
          offloadExecutorByteBuf.retain(numLambda);

          for (int i = 0; i < numLambda; i++) {
            final int rid = requestIdCnt.getAndIncrement();
            requestIdExecutorMap.put(rid, m.getExecutorId());
            requestWorkerInitMap.put(rid, offloadExecutorByteBuf);

            executorService.execute(() -> {
              offloadingRequester.createChannelRequest(workerControlTransport.getPublicAddress(),
                workerControlTransport.getPort(), rid, m.getExecutorId());

            });
          }

          break;
        }
      }
    }

    @Override
    public void onMessageWithContext(ControlMessage.Message message, MessageContext messageContext) {

    }
  }
}
