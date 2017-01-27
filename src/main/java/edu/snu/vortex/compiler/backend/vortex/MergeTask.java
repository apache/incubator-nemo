package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.frontend.beam.element.Element;
import edu.snu.vortex.compiler.frontend.beam.element.Record;
import edu.snu.vortex.compiler.frontend.beam.element.SerializedChunk;
import edu.snu.vortex.runtime.Channel;
import edu.snu.vortex.runtime.Task;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MergeTask extends Task {
  private final Map<BoundedWindow, Map<Object, List>> windowToDataMap;
  private final AtomicInteger pendingInChans; // hack: we assume global watermarks

  public MergeTask(final List<Channel> inChans,
                   final Channel outChan) {
    super(inChans, Arrays.asList(outChan));
    this.windowToDataMap = new HashMap<>();
    this.pendingInChans = new AtomicInteger(inChans.size());
  }

  @Override
  public void compute() {
    getInChans().forEach(inChan -> inChan.read().forEach(input -> {
      final Element<KV> element = (Element<KV>)input;
      System.out.println("MERGE READ: " + element);
      if (element.isWatermark()) {
        pendingInChans.decrementAndGet();
        System.out.println(windowToDataMap);
      } else {
        final SerializedChunk<KV> serializedChunk = (SerializedChunk<KV>)element;
        serializedChunk.getWinVals().forEach(wv -> {
          final BoundedWindow window = (BoundedWindow)wv.getWindows().iterator().next();
          final KV kv = (KV)wv.getValue();
          windowToDataMap.putIfAbsent(window, new HashMap<Object, List>());
          final Map<Object, List> dataMap = windowToDataMap.get(window);
          dataMap.putIfAbsent(kv.getKey(), new ArrayList());
          dataMap.get(kv.getKey()).add(kv.getValue());
        });

      }
    }));


    if (pendingInChans.get() <= 0) {
      final List<Element> result = windowToDataMap.entrySet().stream()
          .flatMap(outerEntry -> {
            final GlobalWindow window = (GlobalWindow) outerEntry.getKey();
            final Map<Object, List> dataMap = outerEntry.getValue();
            return dataMap.entrySet().stream()
                .map(entry -> KV.of(entry.getKey(), entry.getValue()))
                .map(kv -> new Record<>(WindowedValue.of(kv, window.maxTimestamp(), window, PaneInfo.ON_TIME_AND_ONLY_FIRING)));
          })
          .collect(Collectors.toList());
      getOutChans().get(0).write(result);
    } else {
      getOutChans().get(0).write(new ArrayList(0)); // zero-element
    }
  }
}

