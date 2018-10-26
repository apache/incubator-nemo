package org.apache.nemo.common.coder;

import java.io.*;

/**
 * A {@link EncoderFactory} which is used for an array of bytes.
 */
public final class BytesEncoderFactory implements EncoderFactory<byte[]> {

  private static final BytesEncoderFactory BYTES_ENCODER_FACTORY = new BytesEncoderFactory();

  /**
   * A private constructor.
   */
  private BytesEncoderFactory() {
    // do nothing.
  }

  /**
   * Static initializer of the encoder.
   */
  public static BytesEncoderFactory of() {
    return BYTES_ENCODER_FACTORY;
  }

  @Override
  public Encoder<byte[]> create(final OutputStream outputStream) {
    return new BytesEncoder(outputStream);
  }

  /**
   * BytesEncoder.
   */
  private final class BytesEncoder implements Encoder<byte[]> {

    private final OutputStream outputStream;

    /**
     * Constructor.
     *
     * @param outputStream the output stream to store the encoded bytes.
     */
    private BytesEncoder(final OutputStream outputStream) {
      this.outputStream = outputStream;
    }

    @Override
    public void encode(final byte[] value) throws IOException {
      // Write the byte[] as is.
      // Because this interface use the length of byte[] element,
      // the element must not have any padding bytes.
      outputStream.write(value);
    }
  }
}
