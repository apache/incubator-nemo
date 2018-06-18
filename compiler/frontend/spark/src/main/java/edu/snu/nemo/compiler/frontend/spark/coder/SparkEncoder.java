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

import edu.snu.nemo.common.coder.Encoder;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import scala.reflect.ClassTag$;

import java.io.OutputStream;

/**
 * Spark Encoder for serialization.
 * @param <T> type of the object to serialize.
 */
public final class SparkEncoder<T> implements Encoder<T> {
  private final Serializer serializer;

  /**
   * Default constructor.
   *
   * @param serializer Spark serializer.
   */
  public SparkEncoder(final Serializer serializer) {
    this.serializer = serializer;
  }

  @Override
  public EncoderInstance<T> getEncoderInstance(final OutputStream outputStream) {
    return new SparkEncoderInstance<>(outputStream, serializer.newInstance());
  }

  /**
   * SparkEncoderInstance.
   * @param <T2> type of the object to serialize.
   */
  private final class SparkEncoderInstance<T2> implements EncoderInstance<T2> {

    private final SerializationStream out;

    /**
     * Constructor.
     *
     * @param outputStream            the output stream to store the encoded bytes.
     * @param sparkSerializerInstance the actual spark serializer instance to use.
     */
    private SparkEncoderInstance(final OutputStream outputStream,
                                 final SerializerInstance sparkSerializerInstance) {
      this.out = sparkSerializerInstance.serializeStream(outputStream);
    }

    @Override
    public void encode(final T2 element) {
      out.writeObject(element, ClassTag$.MODULE$.Any());
    }
  }
}
