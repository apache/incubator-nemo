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
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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

  @Inject
  private OffloadingWorkerManager(final TcpPortProvider tcpPortProvider,
                                  final EvalConf evalConf,
                                  final ExecutorRegistry executorRegistry,
                                  final PipeIndexMaster pipeIndexMaster,
                                  final SerializedTaskMap serializedTaskMap,
                                  final OffloadingRequester offloadingRequester,
                                  final MessageEnvironment messageEnvironment) {
    this.evalConf = evalConf;
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
      new NioEventLoopGroup(10,
        new DefaultThreadFactory("WorkerControlTransport")),
      false);

    messageEnvironment
      .setupListener(MessageEnvironment.LAMBDA_OFFLOADING_REQUEST_ID,
        new MessageReceiver());

    channelThread.execute(() -> {
      while (!finished) {
        try {
          final Pair<Integer, Pair<Channel, OffloadingMasterEvent>> event =
            nemoEventHandler.getHandshakeQueue().take();

          final int requestId = event.left();
          final Pair<Channel, OffloadingMasterEvent> pair = event.right();
          final ByteBuf workerInitBuffer = requestWorkerInitMap.remove(requestId);

          if (workerInitBuffer == null) {
            throw new RuntimeException("No worker init buffer for request id " + requestId);
          }

          LOG.info("Channel for requestId {}: {}", requestId, pair.left());
          requestIdControlChannelMap.put(requestId, pair.left());

          channelEventHandlerMap.put(pair.left(), new EventHandler<OffloadingMasterEvent>() {
              @Override
              public void onNext(OffloadingMasterEvent msg) {
                switch (msg.getType()) {
                  case CPU_LOAD: {
                    final double load = msg.getByteBuf().readDouble();
                    LOG.info("Receive cpu load {}", load);
                    // cpuAverage.addValue(load);
                    break;
                  }
                  case END:
                    if (Constants.enableLambdaLogging) {
                      LOG.info("Receive end");
                    }
                    msg.getByteBuf().release();
                    // endQueue.add(msg);
                    break;
                  default:
                    throw new RuntimeException("Invalid type: " + msg);
                }
              }
            });

          LOG.info("Waiting worker init.. send buffer {}", workerInitBuffer.readableBytes());
          pair.left().writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.WORKER_INIT, workerInitBuffer));

          final Pair<Channel, OffloadingMasterEvent> workerDonePair = nemoEventHandler.getWorkerReadyQueue().take();
          final int port = workerDonePair.right().getByteBuf().readInt();
          workerDonePair.right().getByteBuf().release();
          final String addr = workerDonePair.left().remoteAddress().toString().split(":")[0];

          LOG.info("Send data channel address to executor {}: {}/{}:{}",
            requestIdExecutorMap.get(requestId),
            requestId, addr, port);

          final String fullAddr = addr + ":" + port;

          final ExecutorRepresenter er =
            executorRegistry.getExecutorRepresentor(requestIdExecutorMap.get(requestId));

          er.sendControlMessage(ControlMessage.Message.newBuilder()
            .setId(RuntimeIdManager.generateMessageId())
            .setListenerId(MessageEnvironment.LAMBDA_OFFLOADING_REQUEST_ID)
            .setType(ControlMessage.MessageType.LambdaControlChannel)
            .setGetLambaControlChannelMsg(ControlMessage.GetLambdaControlChannel.
              newBuilder()
              .setFullAddr(fullAddr)
              .setRequestId(requestId)
              .build())
            .build());

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

  private final Map<Integer, ByteBuf> requestWorkerInitMap = new ConcurrentHashMap<>();
  private final AtomicInteger requestId = new AtomicInteger();
  private final Map<Integer, Channel> requestIdControlChannelMap = new ConcurrentHashMap<>();
  private final Map<Integer, String> requestIdExecutorMap = new ConcurrentHashMap<>();

  public final class MessageReceiver implements MessageListener<ControlMessage.Message> {

    private final ExecutorService executorService = Executors.newFixedThreadPool(50);
    private final Map<String, ByteBuf> offloadExecutorByteBufMap = new ConcurrentHashMap<>();

    @Override
    public void onMessage(ControlMessage.Message message) {
      switch (message.getType()) {
        case LambdaEnd: {
          final int requestId = (int) message.getLambdaEndMsg().getRequestId();
          final Channel channel = requestIdControlChannelMap.get(requestId);
          LOG.info("Lambda End of {}/{}", requestId, channel.remoteAddress());
          channel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.END, null));
          break;
        }
        case TaskSendToLambda: {
          final ControlMessage.TaskSendToLambdaMessage m = message.getTaskSendToLambdaMsg();

          executorService.execute(() -> {

            final int requestId = (int) m.getRequestId();
            final Channel channel = requestIdControlChannelMap.get(requestId);

            final SendToOffloadingWorker taskSend =
              new SendToOffloadingWorker(m.getTaskId(),
                serializedTaskMap.getSerializedTask(m.getTaskId()),
                pipeIndexMaster.getIndexMapForTask(m.getTaskId()), true);
            final ByteBuf byteBuf = channel.alloc().ioBuffer();
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

            LOG.info("Task send {} to lambda {}", m.getTaskId(), channel.remoteAddress());

            channel.writeAndFlush(new OffloadingMasterEvent(
              OffloadingMasterEvent.Type.TASK_SEND, bos.buffer()));
          });

          break;
        }
        case LambdaCreate: {

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
            try {
              final ObjectOutputStream oos = new ObjectOutputStream(bos);
              oos.writeObject(offloadingExecutor);
              oos.writeObject(ser.getInputDecoder());
              oos.writeObject(ser.getOutputEncoder());
              oos.close();
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
            final int rid = requestId.getAndIncrement();
            requestIdExecutorMap.put(rid, m.getExecutorId());

            executorService.execute(() -> {
              offloadingRequester.createChannelRequest(workerControlTransport.getPublicAddress(),
                workerControlTransport.getPort(), rid, m.getExecutorId());

              requestWorkerInitMap.put(rid, offloadExecutorByteBuf);
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
