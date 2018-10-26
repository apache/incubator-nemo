package org.apache.nemo.common.coder;

import org.apache.nemo.common.DirectByteArrayOutputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link DecoderFactory} which is used for an array of bytes.
 */
public final class BytesDecoderFactory implements DecoderFactory<byte[]> {

  private static final BytesDecoderFactory BYTES_DECODER_FACTORY = new BytesDecoderFactory();

  /**
   * A private constructor.
   */
  private BytesDecoderFactory() {
    // do nothing.
  }

  /**
   * Static initializer of the decoder.
   */
  public static BytesDecoderFactory of() {
    return BYTES_DECODER_FACTORY;
  }

  @Override
  public Decoder<byte[]> create(final InputStream inputStream) {
    return new BytesDecoder(inputStream);
  }

  /**
   * BytesDecoder.
   */
  private final class BytesDecoder implements Decoder<byte[]> {

    private final InputStream inputStream;
    private boolean returnedArray;

    /**
     * Constructor.
     *
     * @param inputStream  the input stream to decode.
     */
    private BytesDecoder(final InputStream inputStream) {
      this.inputStream = inputStream;
      this.returnedArray = false;
    }

    @Override
    public byte[] decode() throws IOException {
      // We cannot use inputStream.available() to know the length of bytes to read.
      // The available method only returns the number of bytes can be read without blocking.
      final DirectByteArrayOutputStream byteOutputStream = new DirectByteArrayOutputStream();
      int b = inputStream.read();
      while (b != -1) {
        byteOutputStream.write(b);
        b = inputStream.read();
      }

      final int lengthToRead = byteOutputStream.getCount();
      if (lengthToRead == 0) {
        if (!returnedArray) {
          returnedArray = true;
          return new byte[0];
        } else {
          throw new IOException("EoF (empty partition)!"); // TODO #120: use EOF exception instead of IOException.
        }
      }
      final byte[] resultBytes = new byte[lengthToRead]; // Read the size of this byte array.
      System.arraycopy(byteOutputStream.getBufDirectly(), 0, resultBytes, 0, lengthToRead);

      returnedArray = true;
      return resultBytes;
    }
  }
}
