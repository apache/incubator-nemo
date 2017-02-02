package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.frontend.beam.operator.UnboundedSourceImpl;
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
    this.unbounded = (reader instanceof UnboundedSourceImpl.Reader);
  }

  public boolean isUnbounded() {
    return this.unbounded;
  }

  @Override
  public void compute() {
    System.out.println("GOGO");
    getOutChans().forEach(chan -> {
      try {
        final List read = (List)reader.read();
        if (read.size() > 0) {
          // System.out.println("fromKafka: " + read);
        }
        chan.write(read);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
  }
}

