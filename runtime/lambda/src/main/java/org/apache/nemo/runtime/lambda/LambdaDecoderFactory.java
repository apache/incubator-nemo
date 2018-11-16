package org.apache.nemo.runtime.lambda;

import org.apache.nemo.common.coder.DecoderFactory;

import java.io.IOException;
import java.io.InputStream;

public final class LambdaDecoderFactory implements DecoderFactory {

  private final DecoderFactory valueDecoderFactory;

  public LambdaDecoderFactory(final DecoderFactory decoderFactory) {
    this.valueDecoderFactory = decoderFactory;
  }

  @Override
  public Decoder create(InputStream inputStream) throws IOException {
    return valueDecoderFactory.create(inputStream);
  }

  @Override
  public String toString() {
    return valueDecoderFactory.toString();
  }
}
