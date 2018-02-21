package edu.snu.nemo.runtime.common.message.ncs;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import org.apache.reef.io.serialization.Codec;

/**
 * Codec for ControlMessage.
 */
final class ControlMessageCodec implements Codec<ControlMessage.Message>,
    org.apache.reef.wake.remote.Codec<ControlMessage.Message> {

  ControlMessageCodec() {
  }

  @Override
  public byte[] encode(final ControlMessage.Message obj) {
    return obj.toByteArray();
  }

  @Override
  public ControlMessage.Message decode(final byte[] buf) {
    try {
      return ControlMessage.Message.parseFrom(buf);
    } catch (final InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
