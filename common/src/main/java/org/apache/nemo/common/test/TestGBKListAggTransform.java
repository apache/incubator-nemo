package org.apache.nemo.common.test;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.offloading.common.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public final class TestGBKListAggTransform implements Transform<Pair<Integer, List<Integer>>, Pair<Integer, Integer>> {

  private static final Logger LOG = LoggerFactory.getLogger(TestGBKListAggTransform.class.getName());

  public final Map<Integer, List<Integer>> map = new HashMap<>();

  private Context context;
  private OutputCollector outputCollector;

  @Override
  public void restore() {
    final StateStore stateStore = context.getStateStore();
    if (stateStore.containsState(context.getTaskId())) {
      LOG.info("Restore state for TestGBKAgg in {}", context.getTaskId());
      final InputStream is = stateStore.getStateStream(context.getTaskId());
      final Map<Integer, List<Integer>> s = SerializationUtils.deserialize(is);
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
  public void prepare(Context context, OutputCollector outputCollector) {
    this.context = context;
    this.outputCollector = outputCollector;
    restore();
  }

  @Override
  public void checkpoint() {
    LOG.info("Checkpoint state for TestGBKAgg in {}", context.getTaskId());
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

  private Integer sum(final List<Integer> l) {
    int s = 0;
    for (int elem : l) {
      s += elem;
    }
    return s;
  }

  @Override
  public void onData(Pair<Integer, List<Integer>> element) {

    if (map.containsKey(element.left())) {
      map.get(element.left()).add(sum(element.right()));
    } else {
      map.put(element.left(), new LinkedList<>());
      map.get(element.left()).add(sum(element.right()));
    }

    outputCollector.emit(Pair.of(element.left(), map.get(element.left())));

    if (map.get(element.left()).size() % 10 == 0) {
      Collections.sort(map.get(element.left()));
      LOG.info(" TestListAggGBK in {},  agg result: {} / {}",
        context.getTaskId(), map.get(element.left()).size(), map.get(element.left()));
    }
  }

  @Override
  public void onWatermark(Watermark watermark) {
    LOG.info("Receive watermark {} from TestListAggGBK in {}", watermark.getTimestamp(), context.getTaskId());
    outputCollector.emitWatermark(watermark);
  }

  @Override
  public void close() {

  }
}
