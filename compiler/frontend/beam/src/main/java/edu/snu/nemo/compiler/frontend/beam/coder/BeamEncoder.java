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

import edu.snu.nemo.common.coder.Encoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VoidCoder;

import java.io.IOException;
import java.io.OutputStream;

/**
 * {@link Encoder} from {@link org.apache.beam.sdk.coders.Coder}.
 * @param <T> the type of element to encode.
 */
public final class BeamEncoder<T> implements Encoder<T> {
  private final org.apache.beam.sdk.coders.Coder<T> beamCoder;

  /**
   * Constructor of BeamEncoder.
   *
   * @param beamCoder actual Beam coder to use.
   */
  public BeamEncoder(final org.apache.beam.sdk.coders.Coder<T> beamCoder) {
    this.beamCoder = beamCoder;
  }

  @Override
  public EncoderInstance<T> getEncoderInstance(final OutputStream outputStream) {
    return new BeamEncoderInstance<>(outputStream, beamCoder);
  }

  /**
   * BeamEncoderInstance.
   * @param <T2> the type of element to encode.
   */
  private final class BeamEncoderInstance<T2> implements EncoderInstance<T2> {

    private final org.apache.beam.sdk.coders.Coder<T2> beamCoder;
    private final OutputStream out;

    /**
     * Constructor.
     *
     * @param outputStream the output stream to store the encoded bytes.
     * @param beamCoder    the actual beam coder to use.
     */
    private BeamEncoderInstance(final OutputStream outputStream,
                                final org.apache.beam.sdk.coders.Coder<T2> beamCoder) {
      this.out = outputStream;
      this.beamCoder = beamCoder;
    }

    @Override
    public void encode(final T2 element) throws IOException {
      if (beamCoder instanceof VoidCoder) {
        out.write(0);
        return;
      }
      try {
        beamCoder.encode(element, out);
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
