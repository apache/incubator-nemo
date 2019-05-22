package org.apache.nemo.runtime.lambdaexecutor.middle;

import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.offloading.common.OffloadingEncoder;
import org.apache.nemo.offloading.common.OffloadingSerializer;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.lambdaexecutor.StatelessOffloadingInputDecoder;
import org.apache.nemo.runtime.lambdaexecutor.StatelessOffloadingOutputDecoder;
import org.apache.nemo.runtime.lambdaexecutor.StatelessOffloadingOutputEncoder;

import java.util.Map;

public class MiddleOffloadingSerializer implements OffloadingSerializer {


  private final OffloadingDecoder inputDecoder;
  private final OffloadingEncoder outputEncoder;
  private final OffloadingDecoder outputDecoder;

  public MiddleOffloadingSerializer(final Map<String, Serializer> serializerMap) {
    this.inputDecoder = new MiddleOffloadingInputDecoder(serializerMap);
    this.outputEncoder = new MiddleOffloadingOutputEncoder(null);
    this.outputDecoder = new MiddleOffloadingOutputDecoder(null);
  }

  @Override
  public OffloadingEncoder getInputEncoder() {
    return null;
  }

  @Override
  public OffloadingDecoder getInputDecoder() {
    return inputDecoder;
  }

  @Override
  public OffloadingEncoder getOutputEncoder() {
    return outputEncoder;
  }

  @Override
  public OffloadingDecoder getOutputDecoder() {
    return outputDecoder;
  }


}
