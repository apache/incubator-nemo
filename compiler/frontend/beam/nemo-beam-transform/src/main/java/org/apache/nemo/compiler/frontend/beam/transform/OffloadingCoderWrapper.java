package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.sdk.coders.Coder;
import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.offloading.common.OffloadingEncoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class OffloadingCoderWrapper<T> implements OffloadingEncoder<T>, OffloadingDecoder<T> {

  final Coder<T> beamCoder;

  public OffloadingCoderWrapper(final Coder<T> beamCoder) {
    this.beamCoder = beamCoder;
  }

  @Override
  public void encode(T element, OutputStream outputStream) throws IOException {
    beamCoder.encode(element, outputStream);
  }

  @Override
  public T decode(InputStream inputStream) throws IOException {
    return beamCoder.decode(inputStream);
  }
}
