package org.apache.nemo.runtime.executor;


import io.netty.buffer.ByteBufAllocator;
import org.apache.nemo.common.coder.IntDecoderFactory;
import org.apache.nemo.common.coder.IntEncoderFactory;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class ExecutorThreadTest {

  private final Serializer serializer = new Serializer(IntEncoderFactory.of(), IntDecoderFactory.of(),
    Collections.emptyList(), Collections.emptyList());

  @Test
  public void testExecutorThread() throws InterruptedException {
    final ExecutorThread executorThread = new ExecutorThread(0,
      "executor1", controlEventHandler);

    final List<Object> input = new LinkedList<>();
    input.addAll(Arrays.asList(0, 1, 2, 3, 4, 5));

    final ExecutorThreadTask src1 = new TestExecutorThreadSourceTask("t1", input);

    // handle source task test
    executorThread.addNewTask(src1);
    executorThread.start();

    Thread.sleep(1000);
    assertTrue(input.isEmpty());

    // add new task test
    final List<Object> input2 = new LinkedList<>();
    input2.addAll(Arrays.asList(0, 1, 2, 3, 4, 5));
    final ExecutorThreadTask src2 = new TestExecutorThreadSourceTask("t2", input2);
    executorThread.addNewTask(src2);
    Thread.sleep(1000);
    assertTrue(input2.isEmpty());

    // handle intermediate task test
    final ExecutorThreadTask inter1 = new TestExecutorThreadIntermediateTask("t3");
    executorThread.addNewTask(inter1);

    final DataFetcher d1 = mock(DataFetcher.class);
    when(d1.getEdgeId()).thenReturn("d1");
    final DataFetcher d2 = mock(DataFetcher.class);
    when(d2.getEdgeId()).thenReturn("d2");

    for (int i = 0; i < 1000; i++) {

      if (i == 500) {
        executorThread.addEvent(new TaskControlMessage(
          TaskControlMessage.TaskControlMessageType.PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK, 0, 0, "t1", null));
      } else {
        executorThread.addEvent(new TaskHandlingDataEvent("t3", d1.getEdgeId(),
          0,
          ByteBufAllocator.DEFAULT.buffer().writeInt(i), serializer));
        executorThread.addEvent(new TaskHandlingDataEvent("t3", d2.getEdgeId(),
          0,
          ByteBufAllocator.DEFAULT.buffer().writeInt(i), serializer));
      }
    }

    Thread.sleep(1000);
    final List<Object> result1 = ((TestExecutorThreadIntermediateTask) inter1).dataFetcherListMap.get(d1);
    final List<Object> result2 = ((TestExecutorThreadIntermediateTask) inter1).dataFetcherListMap.get(d2);

    assertEquals(999, result1.size());
    assertEquals(999, result2.size());

    assertEquals(1, controlEventList.size());

    // add shortcut
    executorThread.addShortcutEvent(new TaskControlMessage(
          TaskControlMessage.TaskControlMessageType.PIPE_OUTPUT_STOP_SIGNAL_BY_DOWNSTREAM_TASK, 0, 0, "t1", null));
    Thread.sleep(1000);
    assertEquals(2, controlEventList.size());



  }

  final class TestExecutorThreadSourceTask implements ExecutorThreadTask {

    private final List<Object> data;
    private final String id;

    public TestExecutorThreadSourceTask(
      final String id,
      final List<Object> data) {
      this.id = id;
      this.data = data;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public boolean isSource() {
      return true;
    }

    @Override
    public boolean isSourceAvailable() {
      return !data.isEmpty();
    }

    @Override
    public boolean hasData() {
      return !data.isEmpty();
    }

    @Override
    public void handleData(String edgeId, TaskHandlingEvent t) {

    }

    @Override
    public boolean handleSourceData() {
      data.remove(0);
      return true;
    }
  }


  final class TestExecutorThreadIntermediateTask implements ExecutorThreadTask {

    private final String id;
    private final Map<String, List<Object>> dataFetcherListMap;

    public TestExecutorThreadIntermediateTask(
      final String id) {
      this.id = id;
      this.dataFetcherListMap = new HashMap<>();
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public boolean isSource() {
      return false;
    }

    @Override
    public boolean isSourceAvailable() {
      throw new RuntimeException("Not supported");
    }

    @Override
    public boolean hasData() {
      return true;
    }

    @Override
    public void handleData(String edgeId, TaskHandlingEvent t) {
      dataFetcherListMap.putIfAbsent(edgeId, new LinkedList<>());
      dataFetcherListMap.get(edgeId).add(t.getData());
    }

    @Override
    public boolean handleSourceData() {
      throw new RuntimeException("Not supported");
    }
  }

  final List<TaskHandlingEvent> controlEventList = new LinkedList<>();
  final ControlEventHandler controlEventHandler = new ControlEventHandler() {

    @Override
    public void handleControlEvent(TaskHandlingEvent event) {
      controlEventList.add(event);
    }
  };
}
