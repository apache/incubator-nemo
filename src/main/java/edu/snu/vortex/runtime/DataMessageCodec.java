package edu.snu.vortex.runtime;

import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.wake.remote.Codec;

public final class DataMessageCodec implements Codec<DataMessage>,
    org.apache.reef.io.serialization.Codec<DataMessage> {

  @Override
  public DataMessage decode(final byte[] bytes) {
    return (DataMessage) SerializationUtils.deserialize(bytes);
  }

  @Override
  public byte[] encode(DataMessage serializable) {
    return SerializationUtils.serialize(serializable);
  }
}
