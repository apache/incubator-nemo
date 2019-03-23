package org.apache.nemo.runtime.lambdaexecutor.kafka;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.offloading.common.OffloadingEncoder;
import org.apache.nemo.offloading.common.OffloadingSerializer;
import org.apache.nemo.runtime.executor.common.Serializer;

import java.util.Map;

public class KafkaOffloadingSerializer implements OffloadingSerializer {


  private final OffloadingDecoder inputDecoder;
  private final OffloadingEncoder outputEncoder;
  private final OffloadingDecoder outputDecoder;

  public KafkaOffloadingSerializer(final Map<String, Serializer> serializerMap,
                                   final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder) {
    this.inputDecoder = new KafkaOffloadingInputDecoder(serializerMap, checkpointMarkCoder);
    this.outputEncoder = new KafkaOffloadingOutputEncoder(serializerMap, checkpointMarkCoder);
    this.outputDecoder = new KafkaOffloadingOutputDecoder(serializerMap, checkpointMarkCoder);
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
