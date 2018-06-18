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
package edu.snu.nemo.runtime.executor.data.streamchainer;

import edu.snu.nemo.common.coder.Decoder;
import edu.snu.nemo.common.coder.Encoder;

import java.util.List;

/**
 * class that contains {@link Encoder}, {@link Decoder} and {@link List} of {@link EncodeStreamChainer}.
 * @param <E> encoder element type.
 * @param <D> decoder element type.
 */
public final class Serializer<E, D> {
  private final Encoder<E> encoder;
  private final Decoder<D> decoder;
  private final List<EncodeStreamChainer> encodeStreamChainers;
  private final List<DecodeStreamChainer> decodeStreamChainers;

  /**
   * Constructor.
   *
   * @param encoder              {@link Encoder}.
   * @param decoder              {@link Decoder}.
   * @param encodeStreamChainers the list of {@link EncodeStreamChainer} to use for encoding.
   * @param decodeStreamChainers the list of {@link DecodeStreamChainer} to use for decoding.
   */
  public Serializer(final Encoder<E> encoder,
                    final Decoder<D> decoder,
                    final List<EncodeStreamChainer> encodeStreamChainers,
                    final List<DecodeStreamChainer> decodeStreamChainers) {
    this.encoder = encoder;
    this.decoder = decoder;
    this.encodeStreamChainers = encodeStreamChainers;
    this.decodeStreamChainers = decodeStreamChainers;
  }

  /**
   * @return the {@link Encoder} to use.
   */
  public Encoder<E> getEncoder() {
    return encoder;
  }

  /**
   * @return the {@link Decoder} to use.
   */
  public Decoder<D> getDecoder() {
    return decoder;
  }

  /**
   * @return the list of {@link EncodeStreamChainer} for encoding.
   */
  public List<EncodeStreamChainer> getEncodeStreamChainers() {
    return encodeStreamChainers;
  }

  /**
   * @return the list of {@link EncodeStreamChainer} for decoding.
   */
  public List<DecodeStreamChainer> getDecodeStreamChainers() {
    return decodeStreamChainers;
  }
}
