package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.ir.operator.Sink;
import edu.snu.vortex.runtime.Channel;
import edu.snu.vortex.runtime.MemoryChannel;
import edu.snu.vortex.runtime.Task;

import java.util.Arrays;
import java.util.List;

public class SinkTask extends Task {
  private final Sink sink;

  public SinkTask(final List<Channel> inChans,
                  final Sink sink) {
    super(inChans, Arrays.asList(new MemoryChannel()));
    this.sink = sink;
  }

  @Override
  public void compute() {
    System.out.println("Write GOGO");
    getInChans().forEach(chan -> {
      final List toWrite = chan.read();
      try {
        sink.getWriter().write(toWrite);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
  }
}

