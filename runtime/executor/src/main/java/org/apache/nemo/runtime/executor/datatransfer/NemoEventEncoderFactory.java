package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.commons.lang.SerializationUtils;
import org.apache.nemo.common.coder.EncoderFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

public final class NemoEventEncoderFactory implements EncoderFactory {

  private final EncoderFactory valueEncoderFactory;

  public NemoEventEncoderFactory(final EncoderFactory valueEncoderFactory) {
    this.valueEncoderFactory = valueEncoderFactory;
  }

  @Override
  public Encoder create(OutputStream outputStream) throws IOException {
    return new NemoEventEncoder(valueEncoderFactory.create(outputStream), outputStream);
  }

  private final class NemoEventEncoder implements EncoderFactory.Encoder {
    private final EncoderFactory.Encoder valueEncoder;
    private final OutputStream outputStream;

    NemoEventEncoder(final EncoderFactory.Encoder valueEncoder,
                     final OutputStream outputStream) {
      this.valueEncoder = valueEncoder;
      this.outputStream = outputStream;
    }

    @Override
    public void encode(Object element) throws IOException {
      final byte[] isWatermark = new byte[1];
      if (element instanceof WatermarkWithIndex) {
        isWatermark[0] = 0x01;
        outputStream.write(isWatermark); // this is watermark
        outputStream.write(SerializationUtils.serialize((Serializable) element));
      } else {
        isWatermark[0] = 0x00;
        outputStream.write(isWatermark); // this is not a watermark
        valueEncoder.encode(element);
      }
    }
  }
}
