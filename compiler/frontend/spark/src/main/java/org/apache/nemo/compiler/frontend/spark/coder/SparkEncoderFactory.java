package org.apache.nemo.compiler.frontend.spark.coder;

import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import scala.reflect.ClassTag$;

import java.io.OutputStream;

/**
 * Spark EncoderFactory for serialization.
 * @param <T> type of the object to serialize.
 */
public final class SparkEncoderFactory<T> implements EncoderFactory<T> {
  private final Serializer serializer;

  /**
   * Default constructor.
   *
   * @param serializer Spark serializer.
   */
  public SparkEncoderFactory(final Serializer serializer) {
    this.serializer = serializer;
  }

  @Override
  public Encoder<T> create(final OutputStream outputStream) {
    return new SparkEncoder<>(outputStream, serializer.newInstance());
  }

  /**
   * SparkEncoder.
   * @param <T2> type of the object to serialize.
   */
  private final class SparkEncoder<T2> implements Encoder<T2> {

    private final SerializationStream out;

    /**
     * Constructor.
     *
     * @param outputStream            the output stream to store the encoded bytes.
     * @param sparkSerializerInstance the actual spark serializer instance to use.
     */
    private SparkEncoder(final OutputStream outputStream,
                         final SerializerInstance sparkSerializerInstance) {
      this.out = sparkSerializerInstance.serializeStream(outputStream);
    }

    @Override
    public void encode(final T2 element) {
      out.writeObject(element, ClassTag$.MODULE$.Any());
    }
  }
}
