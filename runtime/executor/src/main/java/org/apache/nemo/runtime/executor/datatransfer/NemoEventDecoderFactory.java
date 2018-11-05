package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.commons.lang.SerializationUtils;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

public final class NemoEventDecoderFactory implements DecoderFactory {

  private final DecoderFactory valueDecoderFactory;

  public NemoEventDecoderFactory(final DecoderFactory valueDecoderFactory) {
    this.valueDecoderFactory = valueDecoderFactory;
  }

  @Override
  public Decoder create(InputStream inputStream) throws IOException {
    return new NemoEventDecoder(valueDecoderFactory.create(inputStream), inputStream);
  }

  private final class NemoEventDecoder implements DecoderFactory.Decoder {
    private final Logger LOG = LoggerFactory.getLogger(NemoEventDecoder.class.getName());

    private final Decoder valueDecoder;
    private final InputStream inputStream;

    NemoEventDecoder(final Decoder valueDecoder,
                     final InputStream inputStream) {
      this.valueDecoder = valueDecoder;
      this.inputStream = inputStream;
    }

    @Override
    public Object decode() throws IOException {
      final byte[] isWatermark = new byte[1];
      inputStream.read(isWatermark, 0, 1);

      if (isWatermark[0] == 0x01) {
        // this is a watermark
        //LOG.info("Decode watermark");
        final WatermarkWithIndex watermarkWithIndex =
          (WatermarkWithIndex)SerializationUtils.deserialize(inputStream);
        return watermarkWithIndex;
      } else {
        //LOG.info("Decode data");
        return valueDecoder.decode();
      }
    }
  }
}
