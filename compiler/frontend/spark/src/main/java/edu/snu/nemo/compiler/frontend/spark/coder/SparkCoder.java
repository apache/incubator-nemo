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

import edu.snu.nemo.common.coder.Coder;
import org.apache.spark.serializer.Serializer;
import scala.reflect.ClassTag$;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Kryo Spark Coder for serialization.
 * @param <T> type of the object to (de)serialize.
 */
public final class SparkCoder<T> implements Coder<T> {
  private final Serializer serializer;

  /**
   * Default constructor.
   * @param serializer kryo serializer.
   */
  public SparkCoder(final Serializer serializer) {
    this.serializer = serializer;
  }

  @Override
  public void encode(final T element, final OutputStream outStream) throws IOException {
    serializer.newInstance().serializeStream(outStream).writeObject(element, ClassTag$.MODULE$.Any());
  }

  @Override
  public T decode(final InputStream inStream) throws IOException {
    final T obj = (T) serializer.newInstance().deserializeStream(inStream).readObject(ClassTag$.MODULE$.Any());
    return obj;
  }
}
