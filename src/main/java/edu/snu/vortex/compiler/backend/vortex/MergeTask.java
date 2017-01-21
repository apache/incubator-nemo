package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.runtime.Channel;
import edu.snu.vortex.runtime.Task;
import org.apache.beam.sdk.values.KV;

import java.util.*;
import java.util.stream.Collectors;

public class MergeTask extends Task {
  public MergeTask(final List<Channel> inChans,
                   final Channel outChan) {
    super(inChans, Arrays.asList(outChan));
  }

  @Override
  public void compute() {
    final Map<Object, List> resultMap = new HashMap<>();

    getInChans().forEach(inChan -> inChan.read().forEach(element -> {
      final KV kv = (KV)element;
      resultMap.putIfAbsent(kv.getKey(), new ArrayList());
      resultMap.get(kv.getKey()).add(kv.getValue());
    }));

    final List<KV> result = resultMap.entrySet().stream()
        .map(entry -> KV.of(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());

    getOutChans().get(0).write(result);
  }
}

