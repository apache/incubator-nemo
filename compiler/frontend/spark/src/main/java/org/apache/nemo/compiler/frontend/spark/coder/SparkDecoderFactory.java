package org.apache.nemo.compiler.frontend.spark.coder;

import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import scala.reflect.ClassTag$;

import java.io.InputStream;

/**
 * Spark DecoderFactory for serialization.
 * @param <T> type of the object to deserialize.
 */
public final class SparkDecoderFactory<T> implements DecoderFactory<T> {
  private final Serializer serializer;

  /**
   * Default constructor.
   *
   * @param serializer Spark serializer.
   */
  public SparkDecoderFactory(final Serializer serializer) {
    this.serializer = serializer;
  }

  @Override
  public Decoder<T> create(final InputStream inputStream) {
    return new SparkDecoder<>(inputStream, serializer.newInstance());
  }

  /**
   * SparkDecoder.
   * @param <T2> type of the object to deserialize.
   */
  private final class SparkDecoder<T2> implements Decoder<T2> {

    private final DeserializationStream in;

    /**
     * Constructor.
     *
     * @param inputStream             the input stream to decode.
     * @param sparkSerializerInstance the actual spark serializer instance to use.
     */
    private SparkDecoder(final InputStream inputStream,
                         final SerializerInstance sparkSerializerInstance) {
      this.in = sparkSerializerInstance.deserializeStream(inputStream);
    }

    @Override
    public T2 decode() {
      return (T2) in.readObject(ClassTag$.MODULE$.Any());
    }
  }
}
