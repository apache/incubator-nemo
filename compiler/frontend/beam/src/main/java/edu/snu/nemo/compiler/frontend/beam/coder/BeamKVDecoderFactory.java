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
import edu.snu.nemo.common.coder.KVDecoderFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VoidCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * KVDecoderFactory for Beam.
 *
 * @param <T> the type of element to decode.
 */
public final class BeamKVDecoderFactory<T> implements KVDecoderFactory<T> {
  private static final Logger LOG = LoggerFactory.getLogger(BeamDecoderFactory.class);
  private final Coder<T> beamCoder;
  private final DecoderFactory keyDecoderFactory;

  /**
   * Constructor of BeamKVDecoderFactory.
   *
   * @param beamCoder actual Beam coder to use.
   */
  public BeamKVDecoderFactory(final Coder<T> beamCoder,
                              final DecoderFactory keyDecoderFactory) {
    this.beamCoder = beamCoder;
    this.keyDecoderFactory = keyDecoderFactory;
  }

  @Override
  public KVDecoder<T> create(final InputStream inputStream) {
    if (beamCoder instanceof VoidCoder) {
      return new BeamVoidKVDecoder<>(inputStream, beamCoder);
    } else {
      return new BeamKVDecoder<>(inputStream, beamCoder);
    }
  }

  @Override
  public DecoderFactory getKeyDecoderFactory() {
    return keyDecoderFactory;
  }

  /**
   * Abstract class for Beam KV Decoder.
   *
   * @param <T2> the type of element to decode.
   */
  private abstract class BeamAbstractKVDecoder<T2> implements KVDecoder<T2> {
    private final Coder<T2> beamCoder;
    private final InputStream inputStream;

    /**
     * Constructor.
     *
     * @param inputStream the input stream to decode.
     * @param beamCoder   the actual beam coder to use.
     */
    protected BeamAbstractKVDecoder(final InputStream inputStream,
                                    final Coder<T2> beamCoder) {
      this.inputStream = inputStream;
      this.beamCoder = beamCoder;
    }

    /**
     * Decode the actual data internally.
     *
     * @return the decoded data.
     * @throws IOException if fail to decode.
     */
    protected T2 decodeInternal() throws IOException {
      try {
        return beamCoder.decode(inputStream);
      } catch (final CoderException e) {
        throw new IOException(e);
      }
    }

    /**
     * @return the input stream.
     */
    protected InputStream getInputStream() {
      return inputStream;
    }
  }

  /**
   * Beam KV Decoder for non void objects.
   *
   * @param <T2> the type of element to decode.
   */
  private final class BeamKVDecoder<T2> extends BeamAbstractKVDecoder<T2> {

    /**
     * Constructor.
     *
     * @param inputStream the input stream to decode.
     * @param beamCoder   the actual beam coder to use.
     */
    private BeamKVDecoder(final InputStream inputStream,
                          final Coder<T2> beamCoder) {
      super(inputStream, beamCoder);
    }

    @Override
    public T2 decode() throws IOException {
      return decodeInternal();
    }
  }

  /**
   * Beam KV Decoder for {@link VoidCoder}.
   *
   * @param <T2> the type of element to decode.
   */
  private final class BeamVoidKVDecoder<T2> extends BeamAbstractKVDecoder<T2> {

    /**
     * Constructor.
     *
     * @param inputStream the input stream to decode.
     * @param beamCoder   the actual beam coder to use.
     */
    private BeamVoidKVDecoder(final InputStream inputStream,
                              final Coder<T2> beamCoder) {
      super(inputStream, beamCoder);
    }

    @Override
    public T2 decode() throws IOException {
      if (getInputStream().read() == -1) {
        throw new IOException("End of stream reached");
      }
      return decodeInternal();
    }
  }

  @Override
  public String toString() {
    return beamCoder.toString();
  }
}
