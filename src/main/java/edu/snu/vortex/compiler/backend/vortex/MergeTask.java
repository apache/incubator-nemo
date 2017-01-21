package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.runtime.Channel;
import edu.snu.vortex.runtime.Task;
import org.apache.beam.sdk.values.KV;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MergeTask extends Task {
  public MergeTask(final List<Channel> inChans,
                   final Channel outChan) {
    super(inChans, Arrays.asList(outChan));
  }

  @Override
  public void compute() {
    final List<KV> result = new ArrayList<>();
    getInChans().forEach(inChan -> result.addAll(inChan.read()));
    getOutChans().get(0).write(result);
  }
}

