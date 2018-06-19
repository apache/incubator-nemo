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

import edu.snu.nemo.common.coder.DecoderFactory;
import edu.snu.nemo.common.coder.EncoderFactory;

import java.util.List;

/**
 * class that contains {@link EncoderFactory}, {@link DecoderFactory} and {@link List} of {@link EncodeStreamChainer}.
 * @param <E> encoderFactory element type.
 * @param <D> decoderFactory element type.
 */
public final class Serializer<E, D> {
  private final EncoderFactory<E> encoderFactory;
  private final DecoderFactory<D> decoderFactory;
  private final List<EncodeStreamChainer> encodeStreamChainers;
  private final List<DecodeStreamChainer> decodeStreamChainers;

  /**
   * Constructor.
   *
   * @param encoderFactory              {@link EncoderFactory}.
   * @param decoderFactory              {@link DecoderFactory}.
   * @param encodeStreamChainers the list of {@link EncodeStreamChainer} to use for encoding.
   * @param decodeStreamChainers the list of {@link DecodeStreamChainer} to use for decoding.
   */
  public Serializer(final EncoderFactory<E> encoderFactory,
                    final DecoderFactory<D> decoderFactory,
                    final List<EncodeStreamChainer> encodeStreamChainers,
                    final List<DecodeStreamChainer> decodeStreamChainers) {
    this.encoderFactory = encoderFactory;
    this.decoderFactory = decoderFactory;
    this.encodeStreamChainers = encodeStreamChainers;
    this.decodeStreamChainers = decodeStreamChainers;
  }

  /**
   * @return the {@link EncoderFactory} to use.
   */
  public EncoderFactory<E> getEncoderFactory() {
    return encoderFactory;
  }

  /**
   * @return the {@link DecoderFactory} to use.
   */
  public DecoderFactory<D> getDecoderFactory() {
    return decoderFactory;
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
