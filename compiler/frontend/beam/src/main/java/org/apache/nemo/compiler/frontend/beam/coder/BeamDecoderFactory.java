package org.apache.nemo.compiler.frontend.beam.coder;

import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VoidCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * {@link DecoderFactory} from {@link org.apache.beam.sdk.coders.Coder}.
 * @param <T> the type of element to decode.
 */
public final class BeamDecoderFactory<T> implements DecoderFactory<T> {
  private static final Logger LOG = LoggerFactory.getLogger(BeamDecoderFactory.class);

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
    if (beamCoder instanceof VoidCoder) {
      return new BeamVoidDecoder<>(inputStream, beamCoder);
    } else {
      return new BeamDecoder<>(inputStream, beamCoder);
    }
  }

  /**
   * Abstract class for Beam Decoder.
   * @param <T2> the type of element to decode.
   */
  private abstract class BeamAbstractDecoder<T2> implements Decoder<T2> {

    private final Coder<T2> beamCoder;
    private final InputStream inputStream;

    /**
     * Constructor.
     *
     * @param inputStream the input stream to decode.
     * @param beamCoder   the actual beam coder to use.
     */
    protected BeamAbstractDecoder(final InputStream inputStream,
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
   * Beam Decoder for non void objects.
   * @param <T2> the type of element to decode.
   */
  private final class BeamDecoder<T2> extends BeamAbstractDecoder<T2> {

    /**
     * Constructor.
     *
     * @param inputStream the input stream to decode.
     * @param beamCoder   the actual beam coder to use.
     */
    private BeamDecoder(final InputStream inputStream,
                        final Coder<T2> beamCoder) {
      super(inputStream, beamCoder);
    }

    @Override
    public T2 decode() throws IOException {
      return decodeInternal();
    }
  }

  /**
   * Beam Decoder for {@link VoidCoder}.
   * @param <T2> the type of element to decode.
   */
  private final class BeamVoidDecoder<T2> extends BeamAbstractDecoder<T2> {

    /**
     * Constructor.
     *
     * @param inputStream the input stream to decode.
     * @param beamCoder   the actual beam coder to use.
     */
    private BeamVoidDecoder(final InputStream inputStream,
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
