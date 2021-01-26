package org.apache.nemo.runtime.executor.data;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.coder.IntDecoderFactory;
import org.apache.nemo.common.coder.IntEncoderFactory;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.common.message.MessageParameters;
import org.apache.nemo.runtime.executor.ExecutorContextManagerMap;
import org.apache.nemo.runtime.executor.TaskScheduledMapWorker;
import org.apache.nemo.runtime.executor.common.DataFetcher;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.datatransfer.InputReader;
import org.apache.nemo.runtime.executor.common.datatransfer.IteratorWithNumBytes;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.master.PipeManagerMaster;
import org.apache.nemo.runtime.master.scheduler.Scheduler;
import org.apache.nemo.runtime.master.scheduler.StreamingScheduler;
import org.apache.reef.io.network.naming.NameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public final class PipeManagerWorkerTest {

  private static final Tang TANG = Tang.Factory.getTang();
  private NameServer nameServer;
  private PipeManagerMaster pipeManagerMaster;
  private MasterHandler masterHandler;

  private final String executor1 = "executor1";
  private final String executor2 = "executor2";
  private final String executor3 = "executor3";

  private final String task1 = "t1";
  private final String task2 = "t2";
  private final String task3 = "t3";
  private final String task4 = "t4";

  private final String edge1 = "edge1";
  private final String edge2 = "edge2";

  private final Map<Triple<String, String, String>, Integer> pipeIndexMap = new HashMap<>();

  private final Map<String, String> taskScheduledMap = new HashMap<>();

  private final Serializer intSerializer = new Serializer(IntEncoderFactory.of(),
    IntDecoderFactory.of(), Collections.emptyList(), Collections.emptyList());

  @Before
  public void setUp() throws InjectionException {

    // Name server
    nameServer = createNameServer();
    final Injector injector = TANG.newInjector(createPipeManagerMasterConf());

    pipeManagerMaster = injector.getInstance(PipeManagerMaster.class);
    masterHandler = new MasterHandler();

    pipeIndexMap.put(Triple.of(task1, edge1, task2), 1);
    pipeIndexMap.put(Triple.of(task1, edge1, task3), 2);
    pipeIndexMap.put(Triple.of(task1, edge1, task4), 3);

    final MessageEnvironment messageEnvironment = injector.getInstance(MessageEnvironment.class);

    messageEnvironment.setupListener(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID,
      masterHandler);

    messageEnvironment.setupListener(MessageEnvironment.TASK_INDEX_MESSAGE_LISTENER_ID,
      masterHandler);

    messageEnvironment.setupListener(MessageEnvironment.TASK_SCHEDULE_MAP_LISTENER_ID,
      masterHandler);
  }

  @After
  public void tearDown() throws Exception {
    nameServer.close();
  }

  @Test
  public void sendDataTest() throws InjectionException, InterruptedException, IOException {
    // Register two executors
    masterHandler.executorIds.add(executor1);
    masterHandler.executorIds.add(executor2);

    // Schedule tasks
    taskScheduledMap.put(task1, executor1);
    taskScheduledMap.put(task2, executor2);
    taskScheduledMap.put(task3, executor2);

    final Pair<PipeManagerWorker, Injector> pair = createPipeManagerWorker(executor1);
    final PipeManagerWorker pipeManagerWorker1 = pair.left();

    final Pair<PipeManagerWorker, Injector> pair2 = createPipeManagerWorker(executor2);
    final PipeManagerWorker pipeManagerWorker2 = pair2.left();

    pair.right().getInstance(TaskScheduledMapWorker.class).init();
    pair.right().getInstance(ExecutorContextManagerMap.class).init();

    pair2.right().getInstance(TaskScheduledMapWorker.class).init();
    pair2.right().getInstance(ExecutorContextManagerMap.class).init();

    final TestInputReader task1reader = new TestInputReader();
    // pipeManagerWorker1.registerInputPipe(task2, task1, task1reader);

    final TestInputReader task2reader = new TestInputReader();
    pipeManagerWorker2.registerInputPipe(task1, edge1, task2, task2reader);

    final TestInputReader task3reader = new TestInputReader();
    pipeManagerWorker2.registerInputPipe(task1, edge1, task3, task3reader);

    // send data from worker1 to worker2
    pipeManagerWorker1.writeData(task1, edge1, task2, intSerializer, 10);
    pipeManagerWorker1.writeData(task1, edge1, task2, intSerializer, 5);
    pipeManagerWorker1.writeData(task1, edge1, task2, intSerializer, 1);
    pipeManagerWorker1.writeData(task1, edge1, task2, intSerializer, 3);

    pipeManagerWorker1.flush();

    Thread.sleep(2000);

    // check received data
    assertEquals(4, task2reader.list.size());

    ByteBuf byteBuf = (ByteBuf) task2reader.list.remove(0);
    ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
    assertEquals(10, intSerializer.getDecoderFactory().create(bis).decode());

    byteBuf = (ByteBuf) task2reader.list.remove(0);
    bis = new ByteBufInputStream(byteBuf);
    assertEquals(5, intSerializer.getDecoderFactory().create(bis).decode());

    byteBuf = (ByteBuf) task2reader.list.remove(0);
    bis = new ByteBufInputStream(byteBuf);
    assertEquals(1, intSerializer.getDecoderFactory().create(bis).decode());

    byteBuf = (ByteBuf) task2reader.list.remove(0);
    bis = new ByteBufInputStream(byteBuf);
    assertEquals(3, intSerializer.getDecoderFactory().create(bis).decode());

    // Broadcast testing
    pipeManagerWorker1.broadcast(task1, edge1,
      Arrays.asList(task2, task3), intSerializer, 100);
    pipeManagerWorker1.flush();

    Thread.sleep(1000);

    // check received data
    assertEquals(1, task2reader.list.size());
    byteBuf = (ByteBuf) task2reader.list.remove(0);
    bis = new ByteBufInputStream(byteBuf);
    assertEquals(100, intSerializer.getDecoderFactory().create(bis).decode());

    assertEquals(1, task3reader.list.size());
    byteBuf = (ByteBuf) task3reader.list.remove(0);
    bis = new ByteBufInputStream(byteBuf);
    assertEquals(100, intSerializer.getDecoderFactory().create(bis).decode());

    // TODO: check control message
  }

  private NameServer createNameServer() throws InjectionException {
    final Configuration configuration = TANG.newConfigurationBuilder()
      .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
      .build();
    final Injector injector = TANG.newInjector(configuration);
    final LocalAddressProvider localAddressProvider = injector.getInstance(LocalAddressProvider.class);
    final NameServer nameServer = injector.getInstance(NameServer.class);
    return nameServer;
  }

  private Configuration createNameResolverConf() throws InjectionException {
    final Configuration configuration = TANG.newConfigurationBuilder()
      .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
      .build();
    final Injector injector = TANG.newInjector(configuration);
    final LocalAddressProvider localAddressProvider = injector.getInstance(LocalAddressProvider.class);

    final Configuration nameClientConfiguration = NameResolverConfiguration.CONF
      .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, localAddressProvider.getLocalAddress())
      .set(NameResolverConfiguration.NAME_SERVICE_PORT, nameServer.getPort())
      .set(NameResolverConfiguration.IDENTIFIER_FACTORY, StringIdentifierFactory.class)
      .build();
    return nameClientConfiguration;
  }

  private Configuration createGrpcMessageEnvironmentConf(
    final String senderId) {
    return TANG.newConfigurationBuilder()
      .bindNamedParameter(MessageParameters.SenderId.class, senderId)
      .build();
  }

  private Configuration createPipeManagerMasterConf() throws InjectionException {

    final Configuration conf = Configurations.merge(
      TANG.newConfigurationBuilder()
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .bindImplementation(Scheduler.class, StreamingScheduler.class)
      .build(),
      createNameResolverConf(),
      createGrpcMessageEnvironmentConf(MessageEnvironment.MASTER_COMMUNICATION_ID));

    return conf;
  }


  private Pair<PipeManagerWorker, Injector>
  createPipeManagerWorker(final String executorId) throws InjectionException {

    final Configuration conf = TANG.newConfigurationBuilder()
      .bindNamedParameter(JobConf.ExecutorId.class, executorId)
      .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
      .bindImplementation(PipeManagerWorker.class, PipeManagerWorkerImpl.class)
      .build();

    final Configuration nameResolverConf = createNameResolverConf();
    final Configuration grpcConf = createGrpcMessageEnvironmentConf(executorId);

    final Injector injector = TANG.newInjector(Configurations.merge(conf, nameResolverConf, grpcConf));

    final PipeManagerWorker pipeManagerWorker = injector.getInstance(PipeManagerWorker.class);

    injector.bindVolatileInstance(BlockManagerWorker.class, mock(BlockManagerWorker.class));
    final CyclicDependencyHandler dependencyHandler = injector.getInstance(CyclicDependencyHandler.class);

    return Pair.of(pipeManagerWorker, injector);
  }

  final class TestInputReader implements InputReader {

    private final List<Object> list = new LinkedList<>();

    @Override
    public Future<Integer> stop(String taskId) {
      return null;
    }

    @Override
    public void restart() {

    }

    @Override
    public void setDataFetcher(DataFetcher dataFetcher) {

    }

    @Override
    public List<CompletableFuture<IteratorWithNumBytes>> read() {
      return null;
    }

    @Override
    public void addControl() {

    }

    @Override
    public void addData(ByteBuf data) {
      list.add(data);
    }

    @Override
    public IRVertex getSrcIrVertex() {
      return null;
    }

    @Override
    public Serializer getSerializer() {
      return null;
    }

    @Override
    public int getTaskIndex() {
      return 0;
    }
  }

  private class MasterHandler implements MessageListener<ControlMessage.Message> {

    private List<String> executorIds = new LinkedList<>();

    @Override
    public void onMessage(ControlMessage.Message message) {
      System.out.println("Message received '" + message.getType());
      // throw new RuntimeException("Not supported " + message.getType());
    }

    @Override
    public void onMessageWithContext(ControlMessage.Message message, MessageContext messageContext) {
      switch (message.getType()) {
        case CurrentExecutor: {
            messageContext.reply(
              ControlMessage.Message.newBuilder()
                .setId(RuntimeIdManager.generateMessageId())
                .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
                .setType(ControlMessage.MessageType.CurrentExecutor)
                .addAllCurrExecutors(executorIds)
                .build());
          break;
        }
        case CurrentScheduledTask: {
          final Collection<String> c = taskScheduledMap
            .entrySet()
            .stream()
            .map(entry -> entry.getKey() + "," + entry.getValue())
            .collect(Collectors.toList());

          messageContext.reply(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.TASK_SCHEDULE_MAP_LISTENER_ID)
              .setType(ControlMessage.MessageType.CurrentScheduledTask)
              .addAllCurrScheduledTasks(c)
              .build());
          break;
        }
        case RequestTaskIndex: {
          System.out.println(message.getRequestTaskIndexMsg());
          final ControlMessage.RequestTaskIndexMessage requestTaskIndexMessage =
            message.getRequestTaskIndexMsg();

          final String srcTaskId = requestTaskIndexMessage.getSrcTaskId();
          final String edgeId = requestTaskIndexMessage.getEdgeId();
          final String dstTaskId = requestTaskIndexMessage.getDstTaskId();
          final Triple<String, String, String> key = Triple.of(srcTaskId, edgeId, dstTaskId);

          messageContext.reply(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.TASK_INDEX_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.TaskIndexInfo)
              .setTaskIndexInfoMsg(ControlMessage.TaskIndexInfoMessage.newBuilder()
                .setRequestId(message.getId())
                .setTaskIndex(pipeIndexMap.get(key))
                .build())
              .build());
          break;
        }
        default:
          throw new RuntimeException("Not supported " + message.getType());
      }
    }
  }
}
