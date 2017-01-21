package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.ir.operator.Do;
import edu.snu.vortex.runtime.Channel;
import edu.snu.vortex.runtime.Task;

import java.util.List;

public class DoTask extends Task {
  private final Do doOperator;

  public DoTask(final List<Channel> inChans,
                final Do doOperator,
                final List<Channel> outChans) {
    super(inChans, outChans);
    this.doOperator = doOperator;
  }

  @Override
  public void compute() {
    getOutChans().get(0).write((List)doOperator.transform(getInChans().get(0).read(), null));
  }
}


