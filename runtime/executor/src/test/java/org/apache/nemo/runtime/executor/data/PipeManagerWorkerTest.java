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
import org.apache.nemo.runtime.executor.MasterSetupHelper;
import org.apache.nemo.runtime.executor.PipeManagerTestHelper;
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

    pipeIndexMap.put(Triple.of(task1, edge1, task2), 1);
    pipeIndexMap.put(Triple.of(task1, edge1, task3), 2);
    pipeIndexMap.put(Triple.of(task1, edge1, task4), 3);

  }

  @After
  public void tearDown() throws Exception {
    nameServer.close();
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

  }


}
