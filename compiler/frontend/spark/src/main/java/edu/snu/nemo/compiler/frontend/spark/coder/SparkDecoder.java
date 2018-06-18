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
package edu.snu.nemo.compiler.frontend.spark.coder;

import edu.snu.nemo.common.coder.Decoder;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import scala.reflect.ClassTag$;

import java.io.InputStream;

/**
 * Spark Decoder for serialization.
 * @param <T> type of the object to deserialize.
 */
public final class SparkDecoder<T> implements Decoder<T> {
  private final Serializer serializer;

  /**
   * Default constructor.
   *
   * @param serializer Spark serializer.
   */
  public SparkDecoder(final Serializer serializer) {
    this.serializer = serializer;
  }

  @Override
  public DecoderInstance<T> getDecoderInstance(final InputStream inputStream) {
    return new SparkDecoderInstance<>(inputStream, serializer.newInstance());
  }

  /**
   * SparkDecoderInstance.
   * @param <T2> type of the object to deserialize.
   */
  private final class SparkDecoderInstance<T2> implements DecoderInstance<T2> {

    private final DeserializationStream in;

    /**
     * Constructor.
     *
     * @param inputStream             the input stream to decode.
     * @param sparkSerializerInstance the actual spark serializer instance to use.
     */
    private SparkDecoderInstance(final InputStream inputStream,
                                 final SerializerInstance sparkSerializerInstance) {
      this.in = sparkSerializerInstance.deserializeStream(inputStream);
    }

    @Override
    public T2 decode() {
      return (T2) in.readObject(ClassTag$.MODULE$.Any());
    }
  }
}
