package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.runtime.Channel;
import edu.snu.vortex.runtime.Task;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class PartitionTask extends Task {
  public PartitionTask(final Channel inChan,
                       final List<Channel> outChans) {
    super(Arrays.asList(inChan), outChans);
  }

  @Override
  public void compute() {
    final int numOfDsts = getOutChans().size();
    final List<WindowedValue<KV>> kvList = getInChans().get(0).read();
    final List<List<WindowedValue<KV>>> dsts = new ArrayList<>(numOfDsts);
    IntStream.range(0, numOfDsts).forEach(x -> dsts.add(new ArrayList<>()));
    kvList.forEach(wv -> {
      final KV kv = wv.getValue();
      final int dst = Math.abs(kv.getKey().hashCode() % numOfDsts);
      dsts.get(dst).add(wv);
    });
    IntStream.range(0, numOfDsts).forEach(x -> getOutChans().get(x).write(dsts.get(x)));
  }
}

