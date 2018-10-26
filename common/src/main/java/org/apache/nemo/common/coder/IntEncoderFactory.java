package org.apache.nemo.common.coder;

import java.io.*;

/**
 * A {@link EncoderFactory} which is used for an integer.
 */
public final class IntEncoderFactory implements EncoderFactory<Integer> {

  private static final IntEncoderFactory INT_ENCODER_FACTORY = new IntEncoderFactory();

  /**
   * A private constructor.
   */
  private IntEncoderFactory() {
    // do nothing.
  }

  /**
   * Static initializer of the coder.
   */
  public static IntEncoderFactory of() {
    return INT_ENCODER_FACTORY;
  }

  @Override
  public Encoder<Integer> create(final OutputStream outputStream) {
    return new IntEncoder(outputStream);
  }

  /**
   * IntEncoder.
   */
  private final class IntEncoder implements Encoder<Integer> {

    private final DataOutputStream outputStream;

    /**
     * Constructor.
     *
     * @param outputStream the output stream to store the encoded bytes.
     */
    private IntEncoder(final OutputStream outputStream) {
      // If the outputStream is closed well in upper level, it is okay to not close this stream
      // because the DataOutputStream itself will not contain any extra information.
      // (when we close this stream, the output will be closed together.)
      this.outputStream = new DataOutputStream(outputStream);
    }

    @Override
    public void encode(final Integer value) throws IOException {
      outputStream.writeInt(value);
    }
  }
}
