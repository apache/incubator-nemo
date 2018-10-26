package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.nemo.common.ir.OutputCollector;

/**
 * Default output emitter that uses outputCollector.
 * @param <OutputT> output type
 */
public final class DefaultOutputManager<OutputT> implements DoFnRunners.OutputManager {
  private final TupleTag<OutputT> mainOutputTag;
  private final OutputCollector<WindowedValue<OutputT>> outputCollector;

  DefaultOutputManager(final OutputCollector<WindowedValue<OutputT>> outputCollector,
                       final TupleTag<OutputT> mainOutputTag) {
    this.outputCollector = outputCollector;
    this.mainOutputTag = mainOutputTag;
  }

  @Override
  public <T> void output(final TupleTag<T> tag, final WindowedValue<T> output) {
    if (tag.equals(mainOutputTag)) {
      outputCollector.emit((WindowedValue<OutputT>) output);
    } else {
      outputCollector.emit(tag.getId(), output);
    }
  }
}
