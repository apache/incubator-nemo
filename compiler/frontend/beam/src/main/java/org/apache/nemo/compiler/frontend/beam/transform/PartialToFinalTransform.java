package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;

public final class PartialToFinalTransform implements Transform<WindowedValue<KV>, Object> {

  private final Combine.CombineFn combineFn;
  private OutputCollector outputCollector;

  public PartialToFinalTransform(Combine.CombineFn combineFn) {
    this.combineFn = combineFn;
  }

  @Override
  public void prepare(Context context, OutputCollector outputCollector) {
    this.outputCollector = outputCollector;
  }

  @Override
  public void onData(WindowedValue<KV> element) {
    final Object result = combineFn.extractOutput(element.getValue().getValue());
    outputCollector.emit(element.withValue(KV.of(element.getValue().getKey(), result)));
  }

  @Override
  public void onWatermark(Watermark watermark) {
    outputCollector.emitWatermark(watermark);
  }

  @Override
  public void close() {

  }
}
