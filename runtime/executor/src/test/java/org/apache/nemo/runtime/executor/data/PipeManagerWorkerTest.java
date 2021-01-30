package org.apache.nemo.runtime.executor.data;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.executor.ExecutorContextManagerMap;
import org.apache.nemo.runtime.executor.MasterSetupHelper;
import org.apache.nemo.runtime.executor.PipeManagerTestHelper;
import org.apache.nemo.runtime.executor.TaskScheduledMapWorker;
import org.apache.nemo.runtime.executor.common.DataFetcher;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskStopSignalByDownstreamTask;
import org.apache.nemo.runtime.executor.common.datatransfer.InputPipeRegister;
import org.apache.nemo.runtime.executor.common.datatransfer.InputReader;
import org.apache.nemo.runtime.executor.common.datatransfer.IteratorWithNumBytes;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.runtime.executor.datatransfer.PipeInputReader;
import org.apache.nemo.runtime.master.PipeManagerMaster;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage.TaskControlMessageType.PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK;
import static org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage.TaskControlMessageType.PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public final class PipeManagerWorkerTest {

  private static final Tang TANG = Tang.Factory.getTang();

  private final String executor1 = "executor1";
  private final String executor2 = "executor2";
  private final String executor3 = "executor3";

  private final String task1 = "t1";
  private final String task2 = "t2";
  private final String task3 = "t3";
  private final String task4 = "t4";

  private final String edge1 = "edge1";
  private final String edge2 = "edge2";



  private NameServer nameServer;
  private PipeManagerMaster pipeManagerMaster;
  private Map<Triple<String, String, String>, Integer> pipeIndexMap;
  private Map<String, String> taskScheduledMap;
  private List<String> executorIds;
  private final Serializer intSerializer = PipeManagerTestHelper.INT_SERIALIZER;

  @Before
  public void setUp() throws InjectionException {

    // Name server
    final MasterSetupHelper masterSetupHelper = new MasterSetupHelper();
    nameServer = masterSetupHelper.nameServer;
    pipeManagerMaster = masterSetupHelper.pipeManagerMaster;
    executorIds = masterSetupHelper.executorIds;
    taskScheduledMap = masterSetupHelper.taskScheduledMap;
    pipeIndexMap = masterSetupHelper.pipeIndexMap;


  }

  @After
  public void tearDown() throws Exception {
    nameServer.close();
  }

  private void checkPipeInitSignal(final String srcTask,
                              final String edge,
                              final String dstTask,
                              final TestInputReader inputReader) {
    assertEquals(
      // left
      new TaskControlMessage(
        TaskControlMessage.TaskControlMessageType.PIPE_INIT,
        pipeIndexMap.get(Triple.of(srcTask, edge, dstTask)),
        pipeIndexMap.get(Triple.of(dstTask, edge, srcTask)),
        dstTask,
        null),
      // right
      inputReader.list.remove(0));
  }

  private void checkData(final int result, final TestInputReader inputReader) throws IOException {
    final ByteBuf byteBuf = (ByteBuf) inputReader.list.remove(0);
    final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
    assertEquals(result, intSerializer.getDecoderFactory().create(bis).decode());
  }

  private String inputReaderKey(final String src,
                                final String dst) {
    return src +"-" + dst;
  }

  private void scheduleTask(final String scheduleTask,
                            final String executorId,
                            final Map<String, List<String>> dag,
                            final Map<String, TestInputReader> inputReaderMap,
                            final List<Injector> injectors,
                            final PipeManagerWorker pipeManagerWorker) {
    taskScheduledMap.put(scheduleTask, executorId);
    injectors.forEach(injector -> {
      try {
        injector.getInstance(TaskScheduledMapWorker.class).registerTask(scheduleTask, executorId);
      } catch (InjectionException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });

    // find upstream tasks
    final List<String> upstreamTasks =
      dag.entrySet().stream().filter(entry -> entry.getValue().contains(scheduleTask))
      .map(entry -> entry.getKey())
      .collect(Collectors.toList());

    for (final String upstream : upstreamTasks) {
      inputReaderMap.putIfAbsent(inputReaderKey(upstream, scheduleTask), new TestInputReader(scheduleTask));
      pipeManagerWorker.registerInputPipe(upstream, edge1, scheduleTask,
        inputReaderMap.get(inputReaderKey(upstream, scheduleTask)));
    }

    for (final String downstream : dag.get(scheduleTask)) {
      inputReaderMap.putIfAbsent(inputReaderKey(downstream, scheduleTask), new TestInputReader(scheduleTask));
      pipeManagerWorker.registerInputPipe(downstream, edge1, scheduleTask,
        inputReaderMap.get(inputReaderKey(downstream, scheduleTask)));
    }
  }

  @Test
  public void sendControlMessageTest() throws Exception {
    // INIT
    executorIds.add(executor1);
    executorIds.add(executor2);
    executorIds.add(executor3);

    final Map<String, List<String>> DAG = new HashMap<>();
    DAG.put(task1, Arrays.asList(task3, task4));
    DAG.put(task2, Arrays.asList(task3, task4));
    DAG.put(task3, new LinkedList<>());
    DAG.put(task4, new LinkedList<>());

    final Map<String, TestInputReader> inputReaderMap = new HashMap<>();

    // Shuffle
    final AtomicInteger counter = new AtomicInteger();
    pipeIndexMap.put(Triple.of(task1, edge1, task3), counter.getAndIncrement());
    pipeIndexMap.put(Triple.of(task3, edge1, task1), counter.getAndIncrement());

    pipeIndexMap.put(Triple.of(task2, edge1, task3), counter.getAndIncrement());
    pipeIndexMap.put(Triple.of(task3, edge1, task2), counter.getAndIncrement());

    pipeIndexMap.put(Triple.of(task1, edge1, task4), counter.getAndIncrement());
    pipeIndexMap.put(Triple.of(task4, edge1, task1), counter.getAndIncrement());

    pipeIndexMap.put(Triple.of(task2, edge1, task4), counter.getAndIncrement());
    pipeIndexMap.put(Triple.of(task4, edge1, task2), counter.getAndIncrement());

    final Pair<PipeManagerWorker, Injector> pair = PipeManagerTestHelper
      .createPipeManagerWorker(executor1, nameServer);
    final PipeManagerWorker pipeManagerWorker1 = pair.left();

    final Pair<PipeManagerWorker, Injector> pair2 = PipeManagerTestHelper
      .createPipeManagerWorker(executor2, nameServer);
    final PipeManagerWorker pipeManagerWorker2 = pair2.left();

    final Pair<PipeManagerWorker, Injector> pair3 = PipeManagerTestHelper
      .createPipeManagerWorker(executor3, nameServer);
    final PipeManagerWorker pipeManagerWorker3 = pair3.left();


    pair.right().getInstance(TaskScheduledMapWorker.class).init();
    pair.right().getInstance(ExecutorContextManagerMap.class).init();

    pair2.right().getInstance(TaskScheduledMapWorker.class).init();
    pair2.right().getInstance(ExecutorContextManagerMap.class).init();

    pair3.right().getInstance(TaskScheduledMapWorker.class).init();
    pair3.right().getInstance(ExecutorContextManagerMap.class).init();

    final List<Injector> injectors = Arrays.asList(pair.right(), pair2.right(), pair3.right());
    // INIT DONE


    // Schedule tasks
    // t1, t2 -> t3, t4
    // Here, we schedule t3 first
    scheduleTask(task3, executor2, DAG, inputReaderMap, injectors, pipeManagerWorker2);

    final TestInputReader t1tot3 = inputReaderMap.get(inputReaderKey(task1, task3));
    final TestInputReader t2tot3 = inputReaderMap.get(inputReaderKey(task2, task3));

    // Task1 is not scheduled... so the pipe init message should be buffered
    // output pipe t3->t1, t3->2 should be buffered
    final Map<Integer, List<Object>> pendingOutputPipeMap2 =
      Whitebox.getInternalState(pipeManagerWorker2, "pendingOutputPipeMap");
    assertEquals(1, pendingOutputPipeMap2.get(pipeIndexMap.get(Triple.of(task3, edge1, task1))).size());
    assertEquals(1, pendingOutputPipeMap2.get(pipeIndexMap.get(Triple.of(task3, edge1, task2))).size());

    // schedule task 1
    // current scheduled task: 1, 3
    scheduleTask(task1, executor1, DAG, inputReaderMap, injectors, pipeManagerWorker1);

    final TestInputReader t3tot1 = inputReaderMap.get(inputReaderKey(task3, task1));
    final TestInputReader t4tot1 = inputReaderMap.get(inputReaderKey(task4, task1));

    // check pipe init signal
    Thread.sleep(200);
    checkPipeInitSignal(task1, edge1, task3, t1tot3);

    assertEquals(0, t3tot1.list.size());
    // flush pending signal
    pipeManagerWorker2.startOutputPipe(pipeIndexMap.get(Triple.of(task3, edge1, task1)), task3);
    Thread.sleep(200);
    checkPipeInitSignal(task3, edge1, task1, t3tot1);

    // check data
    for (int i = 0; i < 1000; i++) {
      pipeManagerWorker1.writeData(task1, edge1, task3, intSerializer, i);
    }
    Thread.sleep(500);
    for (int i = 0; i < 1000; i++) {
      checkData(i, t1tot3);
    }

    // Test: data sending to not initialized pipe
    for (int i = 0; i < 100; i++) {
      pipeManagerWorker1.writeData(task1, edge1, task4, intSerializer, i);
    }

    // Check pending output: 1 control, 100 data
    final Map<Integer, List<Object>> pendingOutputPipeMap1 =
      Whitebox.getInternalState(pipeManagerWorker1, "pendingOutputPipeMap");
    assertEquals(101, pendingOutputPipeMap1.get(pipeIndexMap.get(Triple.of(task1, edge1, task4))).size());

    // Schedule task4 and init the pipe
    scheduleTask(task4, executor2, DAG, inputReaderMap, injectors, pipeManagerWorker2);
    final TestInputReader t1tot4 = inputReaderMap.get(inputReaderKey(task1, task4));
    final TestInputReader t2tot4 = inputReaderMap.get(inputReaderKey(task2, task4));

    Thread.sleep(200);
    checkPipeInitSignal(task4, edge1, task1, t4tot1);

    pipeManagerWorker1.startOutputPipe(pipeIndexMap.get(Triple.of(task1, edge1, task4)), task1);

    Thread.sleep(500);
    checkPipeInitSignal(task1, edge1, task4, t1tot4);
    for (int i = 0; i < 100; i++) {
      checkData(i, t1tot4);
    }

    // Schedule task2
    scheduleTask(task2, executor1, DAG, inputReaderMap, injectors, pipeManagerWorker1);
    final TestInputReader t3tot2 = inputReaderMap.get(inputReaderKey(task3, task2));
    final TestInputReader t4tot2 = inputReaderMap.get(inputReaderKey(task4, task2));

    Thread.sleep(300);
    checkPipeInitSignal(task2, edge1, task3, t2tot3);
    checkPipeInitSignal(task2, edge1, task4, t2tot4);

    pipeManagerWorker2.startOutputPipe(pipeIndexMap.get(Triple.of(task4, edge1, task2)), task4);
    pipeManagerWorker2.startOutputPipe(pipeIndexMap.get(Triple.of(task3, edge1, task2)), task3);

    Thread.sleep(300);
    checkPipeInitSignal(task3, edge1, task2, t3tot2);
    checkPipeInitSignal(task4, edge1, task2, t4tot2);
    // DONE SCHEDULE ALL Of TASKS
    // DONE SCHEDULE ALL Of TASKS
    // DONE SCHEDULE ALL Of TASKS
    // DONE SCHEDULE ALL Of TASKS
    // DONE SCHEDULE ALL Of TASKS


    // STOP AND RESTART TASKS
    // STOP AND RESTART TASKS
    // STOP AND RESTART TASKS
    // STOP AND RESTART TASKS
    // STOP AND RESTART TASKS
    // send stop signal
    pipeManagerWorker2.sendStopSignalForInputPipes(
      Arrays.asList(task1, task2), edge1, task3);
    pipeManagerWorker2.flush();

    Thread.sleep(1000);

    assertEquals(InputPipeRegister.InputPipeState.WAITING_ACK, pipeManagerWorker2.getInputPipeState(task3));

    assertEquals(1, t3tot1.list.size());
    assertEquals(new TaskControlMessage(PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK,
      pipeIndexMap.get(Triple.of(task3, edge1, task1)),
      pipeIndexMap.get(Triple.of(task1, edge1, task3)),
      task1,
        new TaskStopSignalByDownstreamTask(task3, edge1, task1)), t3tot1.list.remove(0));

    assertEquals(1, t3tot2.list.size());
     assertEquals(new TaskControlMessage(PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK,
       pipeIndexMap.get(Triple.of(task3, edge1, task2)),
       pipeIndexMap.get(Triple.of(task2, edge1, task3)),
       task2,
        new TaskStopSignalByDownstreamTask(task3, edge1, task2)), t3tot2.list.remove(0));


    // TODO: Stop output pipe and test whether the output is buffered or not in broadcst and in writeData
    pipeManagerWorker1.stopOutputPipe(
      pipeIndexMap.get(Triple.of(task1, edge1, task3)), task1);

    Thread.sleep(300);

    assertEquals(1, t1tot3.list.size());
    assertEquals(new TaskControlMessage(PIPE_OUTPUT_STOP_ACK_FROM_UPSTREAM_TASK,
      pipeIndexMap.get(Triple.of(task1, edge1, task3)),
      pipeIndexMap.get(Triple.of(task1, edge1, task3)),
      task3,
        null), t1tot3.list.remove(0));

    // Check buffering
    pipeManagerWorker1.writeData(task1, edge1, task3, intSerializer, 10);
    pipeManagerWorker1.writeData(task1, edge1, task3, intSerializer, 11);
    pipeManagerWorker1.writeData(task1, edge1, task3, intSerializer, 12);
    pipeManagerWorker1.writeData(task1, edge1, task3, intSerializer, 13);
    pipeManagerWorker1.writeData(task1, edge1, task3, intSerializer, 14);
    pipeManagerWorker1.broadcast(task1, edge1, Arrays.asList(task3, task4), intSerializer, 15);
    pipeManagerWorker1.writeData(task1, edge1, task4, intSerializer, 16);

    Thread.sleep(500);
    // the output must be buffered in worker1
    assertTrue(t1tot3.list.isEmpty());

    // broadcast variable sent to t4 because t4 is not stopped
    ByteBuf byteBuf = (ByteBuf) t1tot4.list.remove(0);
    ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
    assertEquals(15, intSerializer.getDecoderFactory().create(bis).decode());
    byteBuf = (ByteBuf) t1tot4.list.remove(0);
    bis = new ByteBufInputStream(byteBuf);
    assertEquals(16, intSerializer.getDecoderFactory().create(bis).decode());


    // check isOUtputStopped
    assertTrue(pipeManagerWorker1.isOutputPipeStopped(task1));

    // TODO: test receiveAndInputStopSignal and check the input pipe state
    assertEquals(InputPipeRegister.InputPipeState.WAITING_ACK, pipeManagerWorker2.getInputPipeState(task3));
    pipeManagerWorker2.receiveAckInputStopSignal(task3, pipeIndexMap.get(Triple.of(task1, edge1, task3)));
    assertEquals(InputPipeRegister.InputPipeState.WAITING_ACK, pipeManagerWorker2.getInputPipeState(task3));
    pipeManagerWorker2.receiveAckInputStopSignal(task3, pipeIndexMap.get(Triple.of(task2, edge1, task3)));
    assertEquals(InputPipeRegister.InputPipeState.STOPPED, pipeManagerWorker2.getInputPipeState(task3));

    // Move task3 to executor3
    scheduleTask(task3, executor3, DAG, inputReaderMap, injectors, pipeManagerWorker3);

    Thread.sleep(300);
    checkPipeInitSignal(task3, edge1, task1, t3tot1);
    checkPipeInitSignal(task3, edge1, task2, t3tot2);

    pipeManagerWorker1.startOutputPipe(pipeIndexMap.get(Triple.of(task1, edge1, task3)), task1);
    pipeManagerWorker1.startOutputPipe(pipeIndexMap.get(Triple.of(task2, edge1, task3)), task2);

    Thread.sleep(500);

    for (int i = 10; i < 15; i++) {
      byteBuf = (ByteBuf) t1tot3.list.remove(0);
      bis = new ByteBufInputStream(byteBuf);
      assertEquals(i, intSerializer.getDecoderFactory().create(bis).decode());
    }
  }


  @Test
  public void sendDataTest() throws InjectionException, InterruptedException, IOException {
    // Register two executors
    executorIds.add(executor1);
    executorIds.add(executor2);

    // Schedule tasks
    taskScheduledMap.put(task2, executor2);
    taskScheduledMap.put(task3, executor2);

    pipeIndexMap.put(Triple.of(task1, edge1, task2), 1);
    pipeIndexMap.put(Triple.of(task1, edge1, task3), 2);
    pipeIndexMap.put(Triple.of(task2, edge1, task1), 3);
    pipeIndexMap.put(Triple.of(task3, edge1, task1), 4);

    final Pair<PipeManagerWorker, Injector> pair = PipeManagerTestHelper
      .createPipeManagerWorker(executor1, nameServer);
    final PipeManagerWorker pipeManagerWorker1 = pair.left();

    final Pair<PipeManagerWorker, Injector> pair2 = PipeManagerTestHelper
      .createPipeManagerWorker(executor2, nameServer);
    final PipeManagerWorker pipeManagerWorker2 = pair2.left();

    pair.right().getInstance(TaskScheduledMapWorker.class).init();
    pair.right().getInstance(ExecutorContextManagerMap.class).init();

    pair2.right().getInstance(TaskScheduledMapWorker.class).init();
    pair2.right().getInstance(ExecutorContextManagerMap.class).init();

    final TestInputReader t1tot2 = new TestInputReader(task2);
    pipeManagerWorker2.registerInputPipe(task1, edge1, task2, t1tot2);

    final TestInputReader t1tot3 = new TestInputReader(task3);
    pipeManagerWorker2.registerInputPipe(task1, edge1, task3, t1tot3);

    // Schedule task later
    // register input pipe for task1
    taskScheduledMap.put(task1, executor1);
    final TestInputReader t2tot1 = new TestInputReader(task1);
    final TestInputReader t3tot1 = new TestInputReader(task1);
    pipeManagerWorker1.registerInputPipe(task2, edge1, task1, t2tot1);
    pipeManagerWorker1.registerInputPipe(task3, edge1, task1, t3tot1);

    // Send info
    pair2.right().getInstance(TaskScheduledMapWorker.class).registerTask(task1, executor1);

    Thread.sleep(500);

    // Receive pipe init messages
    checkPipeInitSignal(task1, edge1, task2, t1tot2);
    checkPipeInitSignal(task1, edge1, task3, t1tot3);

    assertEquals(0, t2tot1.list.size());
    // send pipe init
    pipeManagerWorker2.startOutputPipe(pipeIndexMap.get(Triple.of(task2, edge1, task1)), task2);
    Thread.sleep(300);

    checkPipeInitSignal(task2, edge1, task1, t2tot1);

    // send pipe init
    pipeManagerWorker2.startOutputPipe(pipeIndexMap.get(Triple.of(task3, edge1, task1)), task3);
    Thread.sleep(300);
    checkPipeInitSignal(task3, edge1, task1, t3tot1);

    // send data from worker1 to worker2
    pipeManagerWorker1.writeData(task1, edge1, task2, intSerializer, 10);
    pipeManagerWorker1.writeData(task1, edge1, task2, intSerializer, 5);
    pipeManagerWorker1.writeData(task1, edge1, task2, intSerializer, 1);
    pipeManagerWorker1.writeData(task1, edge1, task2, intSerializer, 3);

    pipeManagerWorker1.flush();

    Thread.sleep(2000);

    // check received data
    assertEquals(4, t1tot2.list.size());

    ByteBuf byteBuf = (ByteBuf) t1tot2.list.remove(0);
    ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
    assertEquals(10, intSerializer.getDecoderFactory().create(bis).decode());

    byteBuf = (ByteBuf) t1tot2.list.remove(0);
    bis = new ByteBufInputStream(byteBuf);
    assertEquals(5, intSerializer.getDecoderFactory().create(bis).decode());

    byteBuf = (ByteBuf) t1tot2.list.remove(0);
    bis = new ByteBufInputStream(byteBuf);
    assertEquals(1, intSerializer.getDecoderFactory().create(bis).decode());

    byteBuf = (ByteBuf) t1tot2.list.remove(0);
    bis = new ByteBufInputStream(byteBuf);
    assertEquals(3, intSerializer.getDecoderFactory().create(bis).decode());

    // Broadcast testing
    pipeManagerWorker1.broadcast(task1, edge1,
      Arrays.asList(task2, task3), intSerializer, 100);
    pipeManagerWorker1.flush();

    Thread.sleep(1000);

    // check received data
    assertEquals(1, t1tot2.list.size());
    byteBuf = (ByteBuf) t1tot2.list.remove(0);
    bis = new ByteBufInputStream(byteBuf);
    assertEquals(100, intSerializer.getDecoderFactory().create(bis).decode());

    assertEquals(1, t1tot3.list.size());
    byteBuf = (ByteBuf) t1tot3.list.remove(0);
    bis = new ByteBufInputStream(byteBuf);
    assertEquals(100, intSerializer.getDecoderFactory().create(bis).decode());

    // TODO: check control message
  }

  final class TestInputReader implements InputReader {

    private final List<Object> list = new LinkedList<>();
    private final String taskId;

    public TestInputReader(final String taskId) {
      this.taskId = taskId;
    }

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
    public String getTaskId() {
      return taskId;
    }

    @Override
    public void addControl(TaskControlMessage message) {
      list.add(message);
    }

    @Override
    public void addData(int pipeIndex, ByteBuf data) {
      list.add(data);
    }

    @Override
    public IRVertex getSrcIrVertex() {
      return null;
    }

  }


}
