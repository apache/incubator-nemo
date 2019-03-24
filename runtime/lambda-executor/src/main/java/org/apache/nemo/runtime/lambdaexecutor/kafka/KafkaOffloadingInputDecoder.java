package org.apache.nemo.runtime.lambdaexecutor.kafka;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public final class KafkaOffloadingInputDecoder implements OffloadingDecoder<KafkaOffloadingInput> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaOffloadingInputDecoder.class.getName());
  final Map<String, Serializer> serializerMap;
  private final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder;

  public KafkaOffloadingInputDecoder(final Map<String, Serializer> serializerMap,
                                     final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder) {
    this.serializerMap = serializerMap;
    this.checkpointMarkCoder = checkpointMarkCoder;
  }

  @Override
  public KafkaOffloadingInput decode(InputStream inputStream) throws IOException {
    final int id = new DataInputStream(inputStream).readInt();
    final UnboundedSource.CheckpointMark checkpointMark = checkpointMarkCoder.decode(inputStream);
    final UnboundedSource unboundedSource = SerializationUtils.deserialize(inputStream);
    return new KafkaOffloadingInput(id, checkpointMark, unboundedSource);
  }
}
