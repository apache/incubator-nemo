package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.ir.operator.Source;
import edu.snu.vortex.runtime.Channel;
import edu.snu.vortex.runtime.Task;
import org.apache.beam.sdk.io.UnboundedSource;

import java.util.List;

public class SourceTask extends Task {
  private final boolean unbounded;
  private final Source.Reader reader;

  public SourceTask(final Source.Reader reader,
                    final List<Channel> outChans) {
    super(null, outChans);
    this.reader = reader;
    this.unbounded = (reader instanceof UnboundedSource.UnboundedReader);
  }

  @Override
  public void compute() {
    while (true) {
      getOutChans().forEach(chan -> {
        try {
          chan.write((List) reader.read());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      if (!unbounded) {
        break;
      } else {
        try {
          synchronized (this) {
            Thread.sleep(10);
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}

