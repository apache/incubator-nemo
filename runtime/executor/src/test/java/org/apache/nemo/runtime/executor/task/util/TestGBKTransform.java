package org.apache.nemo.runtime.executor.task.util;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.StateStore;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public final class TestGBKTransform implements Transform<Pair<Integer, Integer>, Pair<Integer, Integer>> {

  private static final Logger LOG = LoggerFactory.getLogger(TestGBKTransform.class.getName());

  public final Map<Integer, Integer> map = new HashMap<>();

  private Context context;
  private OutputCollector outputCollector;

  @Override
  public void prepare(Context context, OutputCollector outputCollector) {
    this.context = context;
    this.outputCollector = outputCollector;

    final StateStore stateStore = context.getStateStore();
    if (stateStore.containsState(context.getTaskId())) {
      LOG.info("Restore state for TestGBK in {}", context.getTaskId());
      final InputStream is = stateStore.getStateStream(context.getTaskId());
      final Map<Integer, Integer> s = SerializationUtils.deserialize(is);
      map.putAll(s);

      try {
        is.close();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void checkpoint() {
    LOG.info("Checkpoint state for TestGBK in {}", context.getTaskId());
    final StateStore stateStore = context.getStateStore();
    final OutputStream os = stateStore.getOutputStreamForStoreTaskState(context.getTaskId());
    SerializationUtils.serialize((HashMap) map, os);
    try {
      os.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onData(Pair<Integer, Integer> element) {

    if (map.containsKey(element.left())) {
      map.put(element.left(), map.get(element.left()) + element.right());
    } else {
      map.put(element.left(), element.right());
    }

    LOG.info("Receive {} from TestGBK in {}, combine result: {}",
      element, context.getTaskId(), map.get(element.left()));
  }

  @Override
  public void onWatermark(Watermark watermark) {
    LOG.info("Receive {} from TestGBK in {}", watermark.getTimestamp(), context.getTaskId());
  }

  @Override
  public void close() {

  }
}
