package org.apache.nemo.runtime.executor.data.streamchainer;

import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;

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
