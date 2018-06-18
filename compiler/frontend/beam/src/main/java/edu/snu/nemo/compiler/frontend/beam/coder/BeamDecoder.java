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

import edu.snu.nemo.common.coder.Decoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VoidCoder;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@link Decoder} from {@link org.apache.beam.sdk.coders.Coder}.
 * @param <T> the type of element to encode.
 */
public final class BeamDecoder<T> implements Decoder<T> {
  private final org.apache.beam.sdk.coders.Coder<T> beamCoder;

  /**
   * Constructor of BeamDecoder.
   *
   * @param beamCoder actual Beam coder to use.
   */
  public BeamDecoder(final org.apache.beam.sdk.coders.Coder<T> beamCoder) {
    this.beamCoder = beamCoder;
  }

  @Override
  public DecoderInstance<T> getDecoderInstance(final InputStream inputStream) {
    return new BeamDecoderInstance<>(inputStream, beamCoder);
  }

  /**
   * BeamDecoderInstance.
   * @param <T2> the type of element to decode.
   */
  private final class BeamDecoderInstance<T2> implements DecoderInstance<T2> {

    private final org.apache.beam.sdk.coders.Coder<T2> beamCoder;
    private final InputStream in;

    /**
     * Constructor.
     *
     * @param inputStream the input stream to decode.
     * @param beamCoder   the actual beam coder to use.
     */
    private BeamDecoderInstance(final InputStream inputStream,
                                final org.apache.beam.sdk.coders.Coder<T2> beamCoder) {
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
