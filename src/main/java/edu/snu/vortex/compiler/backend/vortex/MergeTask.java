package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.frontend.beam.element.Element;
import edu.snu.vortex.compiler.frontend.beam.element.Record;
import edu.snu.vortex.compiler.frontend.beam.element.SerializedChunk;
import edu.snu.vortex.compiler.frontend.beam.element.Watermark;
import edu.snu.vortex.runtime.Channel;
import edu.snu.vortex.runtime.Task;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MergeTask extends Task {
  private final BitSet allSet;
  private final int numInChans;
  private final Map<BoundedWindow, Map<Object, List>> windowToDataMap;
  private final Map<BoundedWindow, Long> latencyMap = new HashMap<>();
  private final Map<BoundedWindow, BitSet> windowToPendings = new HashMap<>();
  private final Set<BoundedWindow> toFlush;


  public MergeTask(final List<Channel> inChans,
                   final Channel outChan) {
    super(inChans, Arrays.asList(outChan));
    this.windowToDataMap = new HashMap<>();
    this.numInChans = inChans.size();
    this.allSet = new BitSet(numInChans);
    this.allSet.set(0, numInChans);
    this.toFlush = new HashSet<>();
  }

  @Override
  public void compute() {
    final long start = System.currentTimeMillis();
    IntStream.range(0, numInChans).forEach(index -> {
      final Channel inChan = getInChans().get(index);
      inChan.read().forEach(input -> {
        final Element<KV> element = (Element<KV>)input;
        // System.out.println("MERGE READ: " + element);
        if (element.isWatermark()) {
          updatePendings((Watermark)element, index);
        } else {
          final SerializedChunk<KV> serializedChunk = (SerializedChunk<KV>)element;
          updateDataMap(serializedChunk);
        }
      });
    });

    if (toFlush.size() > 0) {
      System.out.print("merge read took: " + (System.currentTimeMillis() - start));
      System.out.println(" | flush: " + toFlush);
      flush();
    } else {
      getOutChans().get(0).write(new ArrayList(0)); // zero-element
    }
  }

  private void updateDataMap(final SerializedChunk<KV> serializedChunk) {
    serializedChunk.getWinVals().forEach(wv -> {
      final BoundedWindow window = (BoundedWindow)wv.getWindows().iterator().next();
      final KV kv = (KV)wv.getValue();
      windowToDataMap.putIfAbsent(window, new HashMap<>());
      latencyMap.putIfAbsent(window, System.currentTimeMillis());
      windowToPendings.putIfAbsent(window, new BitSet(numInChans));
      final Map<Object, List> dataMap = windowToDataMap.get(window);
      dataMap.putIfAbsent(kv.getKey(), new ArrayList());
      dataMap.get(kv.getKey()).add(kv.getValue());
    });
    // System.out.println("after update dataMap: " + windowToDataMap);
  }

  private void updatePendings(final Watermark watermark, final int index) {
    windowToPendings.entrySet().forEach(entry -> {
      final BoundedWindow window = entry.getKey();
      if (watermark.getTimestamp().isAfter(window.maxTimestamp())) {
        final BitSet bitSet = entry.getValue();
        bitSet.set(index);
        if (bitSet.equals(allSet)) {
          toFlush.add(window);
        }
      }
    });
    System.out.println("after update pending: " + windowToPendings);
  }

  private void flush() {
    final List<Element> result = toFlush.stream()
        .flatMap(window -> {
          final Map<Object, List> dataMap = windowToDataMap.remove(window);
          final BitSet bitSet = windowToPendings.remove(window);
          System.out.println("latency(seconds) of " + window + " is: " + ((System.currentTimeMillis() - latencyMap.remove(window)) / 1000));
          return dataMap.entrySet().stream()
              .map(entry -> KV.of(entry.getKey(), entry.getValue()))
              .map(kv -> new Record<>(WindowedValue.of(kv, window.maxTimestamp(), window, PaneInfo.ON_TIME_AND_ONLY_FIRING)));
        })
        .collect(Collectors.toList());
    getOutChans().get(0).write(result);
    toFlush.clear();
  }
}

