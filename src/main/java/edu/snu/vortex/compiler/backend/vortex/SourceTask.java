package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.ir.operator.Source;
import edu.snu.vortex.runtime.Channel;
import edu.snu.vortex.runtime.Task;

import java.util.List;

public class SourceTask extends Task {
  private final Source.Reader reader;

  public SourceTask(final Source.Reader reader,
                    final List<Channel> outChans) {
    super(null, outChans);
    this.reader = reader;
  }

  @Override
  public void compute() {
    getOutChans().forEach(chan -> {
      try {
        chan.write((List) reader.read());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}

