package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * DoFn transform implementation.
 *
 * @param <InputT> input type.
 * @param <OutputT> output type.
 */
public final class DoFnTransform<InputT, OutputT> extends AbstractDoFnTransform<InputT, InputT, OutputT> {

  /**
   * DoFnTransform Constructor.
   *
   * @param doFn    doFn.
   * @param options Pipeline options.
   */
  public DoFnTransform(final DoFn<InputT, OutputT> doFn,
                       final Coder<InputT> inputCoder,
                       final Map<TupleTag<?>, Coder<?>> outputCoders,
                       final TupleTag<OutputT> mainOutputTag,
                       final List<TupleTag<?>> additionalOutputTags,
                       final WindowingStrategy<?, ?> windowingStrategy,
                       final Collection<PCollectionView<?>> sideInputs,
                       final PipelineOptions options) {
    super(doFn, inputCoder, outputCoders, mainOutputTag,
      additionalOutputTags, windowingStrategy, sideInputs, options);
  }

  @Override
  protected DoFn wrapDoFn(final DoFn initDoFn) {
    return initDoFn;
  }

  @Override
  public void onData(final WindowedValue<InputT> data) {
    getDoFnRunner().processElement(data);
  }

  @Override
  protected void beforeClose() {
    // nothing
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("DoTransform:" + getDoFn());
    return sb.toString();
  }
}
