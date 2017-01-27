package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.frontend.beam.element.Element;
import edu.snu.vortex.compiler.frontend.beam.element.SerializedChunk;
import edu.snu.vortex.runtime.Channel;
import edu.snu.vortex.runtime.Task;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
    final List<Element<KV>> inputList = getInChans().get(0).read();
    final List<List<Element<KV>>> dsts = new ArrayList<>(numOfDsts);
    IntStream.range(0, numOfDsts).forEach(x -> dsts.add(new ArrayList<>()));
    final List<SerializedChunk> buffers = new ArrayList<>(numOfDsts);
    IntStream.range(0, numOfDsts).forEach(x -> buffers.add(new SerializedChunk()));

    inputList.forEach(input -> {
      final Element<KV> element = (Element<KV>)input;
      if (element.isWatermark()) {
        // flush and add watermark
        IntStream.range(0, numOfDsts).forEach(x -> {
          final List<Element<KV>> dst = dsts.get(x);
          final SerializedChunk serChunk = buffers.get(x);
          if (!serChunk.isEmpty()) {
            dst.add(serChunk);
            buffers.set(x, new SerializedChunk()); // reset
          }
          dst.add(element);
        });
      } else {
        // add to buffer
        final WindowedValue<KV> windowedValue = element.asRecord().getWindowedValue();
        final KV kv = windowedValue.getValue();
        final int dst = Math.abs(kv.getKey().hashCode() % numOfDsts);
        buffers.get(dst).addWinVal(windowedValue);
      }
    });

    // flushnonempty buffers
    IntStream.range(0, numOfDsts).forEach(x -> {
      final SerializedChunk serChunk = buffers.get(x);
      if (!serChunk.isEmpty()) {
        dsts.get(x).add(serChunk);
      }
    });

    // then write
    IntStream.range(0, numOfDsts).forEach(x -> getOutChans().get(x).write(dsts.get(x)));
  }
}

