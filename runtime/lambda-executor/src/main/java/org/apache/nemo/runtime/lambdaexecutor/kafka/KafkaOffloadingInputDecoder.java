package org.apache.nemo.runtime.lambdaexecutor.kafka;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public final class KafkaOffloadingInputDecoder implements OffloadingDecoder<UnboundedSource.CheckpointMark> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaOffloadingInputDecoder.class.getName());
  final Map<String, Serializer> serializerMap;
  private final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder;

  public KafkaOffloadingInputDecoder(final Map<String, Serializer> serializerMap,
                                     final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder) {
    this.serializerMap = serializerMap;
    this.checkpointMarkCoder = checkpointMarkCoder;
  }

  @Override
  public UnboundedSource.CheckpointMark decode(InputStream inputStream) throws IOException {
    final UnboundedSource.CheckpointMark checkpointMark = checkpointMarkCoder.decode(inputStream);
    return checkpointMark;
  }
}
