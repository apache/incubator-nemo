/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.compiler.frontend.beam.coder;

import edu.snu.nemo.common.coder.DecoderFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VoidCoder;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@link DecoderFactory} from {@link org.apache.beam.sdk.coders.Coder}.
 * @param <T> the type of element to encode.
 */
public final class BeamDecoderFactory<T> implements DecoderFactory<T> {
  private final Coder<T> beamCoder;

  /**
   * Constructor of BeamDecoderFactory.
   *
   * @param beamCoder actual Beam coder to use.
   */
  public BeamDecoderFactory(final Coder<T> beamCoder) {
    this.beamCoder = beamCoder;
  }

  @Override
  public Decoder<T> create(final InputStream inputStream) {
    return new BeamDecoder<>(inputStream, beamCoder);
  }

  /**
   * BeamDecoder.
   * @param <T2> the type of element to decode.
   */
  private final class BeamDecoder<T2> implements Decoder<T2> {

    private final Coder<T2> beamCoder;
    private final InputStream in;

    /**
     * Constructor.
     *
     * @param inputStream the input stream to decode.
     * @param beamCoder   the actual beam coder to use.
     */
    private BeamDecoder(final InputStream inputStream,
                        final Coder<T2> beamCoder) {
      this.in = inputStream;
      this.beamCoder = beamCoder;
    }

    @Override
    public T2 decode() throws IOException {
      if (beamCoder instanceof VoidCoder && in.read() == -1) {
        throw new IOException("End of stream reached");
      }
      try {
        return beamCoder.decode(in);
      } catch (final CoderException e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  public String toString() {
    return beamCoder.toString();
  }
}
