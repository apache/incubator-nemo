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

import edu.snu.nemo.common.coder.EncoderFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VoidCoder;

import java.io.IOException;
import java.io.OutputStream;

/**
 * {@link EncoderFactory} from {@link Coder}.
 * @param <T> the type of element to encode.
 */
public final class BeamEncoderFactory<T> implements EncoderFactory<T> {

  private final Coder<T> beamCoder;

  /**
   * Constructor of BeamEncoderFactory.
   *
   * @param beamCoder actual Beam coder to use.
   */
  public BeamEncoderFactory(final Coder<T> beamCoder) {
    this.beamCoder = beamCoder;
  }

  @Override
  public Encoder<T> create(final OutputStream outputStream) {
    if (beamCoder instanceof VoidCoder) {
      return new BeamVoidEncoder<>(outputStream);
    } else {
      return new BeamEncoder<>(outputStream, beamCoder);
    }
  }

  /**
   * Beam Encoder for non void objects.
   *
   * @param <T2> the type of element to decode.
   */
  private final class BeamEncoder<T2> implements Encoder<T2> {

    private final Coder<T2> beamCoder;
    private final OutputStream outputStream;

    /**
     * Constructor.
     *
     * @param outputStream the output stream to store the encoded bytes.
     * @param beamCoder    the actual beam coder to use.
     */
    private BeamEncoder(final OutputStream outputStream,
                        final Coder<T2> beamCoder) {
      this.outputStream = outputStream;
      this.beamCoder = beamCoder;
    }

    @Override
    public void encode(final T2 element) throws IOException {
      try {
        beamCoder.encode(element, outputStream);
      } catch (final CoderException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Beam Decoder for {@link VoidCoder}.
   *
   * @param <T2> the type of element to decode.
   */
  private final class BeamVoidEncoder<T2> implements Encoder<T2> {

    private final OutputStream outputStream;

    /**
     * Constructor.
     *
     * @param outputStream the output stream to store the encoded bytes.
     */
    private BeamVoidEncoder(final OutputStream outputStream) {
      this.outputStream = outputStream;
    }

    @Override
    public void encode(final T2 element) throws IOException {
      outputStream.write(0); // emit 0 instead of null to enable to count emitted elements.
    }
  }

  @Override
  public String toString() {
    return beamCoder.toString();
  }
}
