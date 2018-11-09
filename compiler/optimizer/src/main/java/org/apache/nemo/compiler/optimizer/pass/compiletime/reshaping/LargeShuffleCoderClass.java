/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.common.DirectByteArrayOutputStream;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Large shuffle coders.
 */
final class LargeShuffleCoderClass implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(LargeShuffleCoderClass.class.getName());

  /**
   * Encoder factory.
   */
  static final class LengthPaddingEncoderFactory implements EncoderFactory {

    private final EncoderFactory valueEncoderFactory;

    LengthPaddingEncoderFactory(final EncoderFactory valueEncoderFactory) {
      this.valueEncoderFactory = valueEncoderFactory;
    }

    @Override
    public Encoder create(final OutputStream outputStream) throws IOException {
      return new LengthPaddingEncoder(valueEncoderFactory, outputStream);
    }
  }

  /**
   * Decoder factory.
   */
  static final class LengthPaddingDecoderFactory implements DecoderFactory {

    LengthPaddingDecoderFactory() {
    }

    @Override
    public Decoder create(final InputStream inputStream) throws IOException {
      return new LengthPaddingDecoder(inputStream);
    }
  }

  /**
   * Encoder.
   * @param <T> type
   */
  private static final class LengthPaddingEncoder<T> implements EncoderFactory.Encoder<T> {

    private final EncoderFactory<T> valueEncoderFactory;
    private final OutputStream outputStream;

    private LengthPaddingEncoder(final EncoderFactory<T> valueEncoderFactory,
                                 final OutputStream outputStream) {
      this.valueEncoderFactory = valueEncoderFactory;
      this.outputStream = outputStream;
    }

    @Override
    public void encode(final T element) throws IOException {
      final DirectByteArrayOutputStream dbos = new DirectByteArrayOutputStream();
      final EncoderFactory.Encoder<T> valueEncoder = valueEncoderFactory.create(dbos);
      // The value encoder will encode the value to the bos
      valueEncoder.encode(element);
      dbos.close();

      int len = dbos.getCount();

      final DataOutputStream dos = new DataOutputStream(outputStream);
      dos.writeInt(len);
      dbos.writeTo(dos);
    }

    @Override
    public String toString() {
      return "{LengthPaddingEncoder}";
    }
  }

  /**
   * Decoder.
   */
  private static final class LengthPaddingDecoder implements DecoderFactory.Decoder {

    private final InputStream inputStream;
    private DirectByteArrayOutputStream byteArrayOutputStream;
    private int len;

    private LengthPaddingDecoder(final InputStream inputStream) {
      this.inputStream = inputStream;
    }

    @Override
    public byte[] decode() throws IOException {
      // this just returns byte array
      final DataInputStream dis = new DataInputStream(inputStream);
      len = dis.readInt();
      byteArrayOutputStream = new DirectByteArrayOutputStream(len);

      while (byteArrayOutputStream.getCount() < len) {
        int b = dis.read();
        if (b == -1) {
          throw new RuntimeException();
        }
        byteArrayOutputStream.write(b);
      }

      return byteArrayOutputStream.getBufDirectly();
    }
  }
}
