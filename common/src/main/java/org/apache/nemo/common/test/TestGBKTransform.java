package org.apache.nemo.common.test;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public final class TestGBKTransform implements Transform<Pair<Integer, Integer>, Pair<Integer, Integer>> {

  private static final Logger LOG = LoggerFactory.getLogger(TestGBKTransform.class.getName());

  public final Map<Integer, Integer> map = new HashMap<>();

  private Context context;
  private OutputCollector outputCollector;

  @Override
  public void restore() {
    final StateStore stateStore = context.getStateStore();
    if (stateStore.containsState(context.getTaskId())) {
      final InputStream is = stateStore.getStateStream(context.getTaskId());
      final Map<Integer, Integer> s = SerializationUtils.deserialize(is);
      map.putAll(s);
      LOG.info("Restore state for TestGBK in {} / {}", context.getTaskId(), map);

      try {
        is.close();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void prepare(Context context, OutputCollector outputCollector) {
    this.context = context;
    this.outputCollector = outputCollector;
    restore();
  }

  @Override
  public void checkpoint() {
    LOG.info("Checkpoint state for TestGBK in {}", context.getTaskId());
    final StateStore stateStore = context.getStateStore();
    final ByteArrayOutputStream bos = new ByteArrayOutputStream(100);
    SerializationUtils.serialize((HashMap) map, bos);
    try {
      bos.close();
      stateStore.put(context.getTaskId(), bos.toByteArray());
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

    outputCollector.emit(Pair.of(element.left(), map.get(element.left())));

    LOG.info("Receive {} from TestGBK in {}, combine result: {}",
      element, context.getTaskId(), map.get(element.left()));
  }

  @Override
  public void onWatermark(Watermark watermark) {
    LOG.info("Receive watermark {} from TestGBK in {}", watermark.getTimestamp(), context.getTaskId());
    outputCollector.emitWatermark(watermark);
  }

  @Override
  public void close() {

  }
}
