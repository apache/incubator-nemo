package org.apache.nemo.runtime.executor.data;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.Pair;
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
import org.apache.nemo.runtime.master.PipeManagerMaster;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

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

  @Test
  public void sendControlMessageTest() throws Exception {
    // Register two executors
    executorIds.add(executor1);
    executorIds.add(executor2);
    executorIds.add(executor3);

    // Schedule tasks
    // t1, t2 -> t3, t4
    taskScheduledMap.put(task1, executor1);
    taskScheduledMap.put(task2, executor1);
    taskScheduledMap.put(task3, executor2);
    taskScheduledMap.put(task4, executor2);

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

    final TestInputReader t1tot3 = new TestInputReader(task1);
    final TestInputReader t1tot4 = new TestInputReader(task1);
    pipeManagerWorker2.registerInputPipe(task1, edge1, task3, t1tot3);
    pipeManagerWorker2.registerInputPipe(task1, edge1, task4, t1tot4);

    final TestInputReader t3tot1 = new TestInputReader(task3);
    final TestInputReader t4tot1 = new TestInputReader(task4);
    pipeManagerWorker1.registerInputPipe(task3, edge1, task1, t3tot1);
    pipeManagerWorker1.registerInputPipe(task4, edge1, task1, t4tot1);

    final TestInputReader t2tot3 = new TestInputReader(task2);
    final TestInputReader t2tot4 = new TestInputReader(task2);
    pipeManagerWorker2.registerInputPipe(task2, edge1, task3, t2tot3);
    pipeManagerWorker2.registerInputPipe(task2, edge1, task4, t2tot4);

    final TestInputReader t3tot2 = new TestInputReader(task3);
    final TestInputReader t4tot2 = new TestInputReader(task4);
    pipeManagerWorker1.registerInputPipe(task3, edge1, task2, t3tot2);
    pipeManagerWorker1.registerInputPipe(task4, edge1, task2, t4tot2);

    // send stop signal
    pipeManagerWorker2.sendSignalForPipes(
      Arrays.asList(task1, task2), edge1, task3, InputPipeRegister.Signal.INPUT_STOP);
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
    taskScheduledMap.put(task3, executor3);
    pair.right().getInstance(TaskScheduledMapWorker.class).registerTask(task3, executor3);
    pair3.right().getInstance(TaskScheduledMapWorker.class).registerTask(task3, executor3);

    pipeManagerWorker3.registerInputPipe(task1, edge1, task3, t1tot3);
    pipeManagerWorker3.registerInputPipe(task2, edge1, task3, t2tot3);

    // TODO: restart output pipe and test whether the buffered output is emitted or not
    pipeManagerWorker1.restartOutputPipe(pipeIndexMap.get(Triple.of(task1, edge1, task3)), task1);

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
    taskScheduledMap.put(task1, executor1);
    taskScheduledMap.put(task2, executor2);
    taskScheduledMap.put(task3, executor2);

    pipeIndexMap.put(Triple.of(task1, edge1, task2), 1);
    pipeIndexMap.put(Triple.of(task1, edge1, task3), 2);

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

    final TestInputReader task1reader = new TestInputReader(task1);
    // pipeManagerWorker1.registerInputPipe(task2, task1, task1reader);

    final TestInputReader task2reader = new TestInputReader(task2);
    pipeManagerWorker2.registerInputPipe(task1, edge1, task2, task2reader);

    final TestInputReader task3reader = new TestInputReader(task3);
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
