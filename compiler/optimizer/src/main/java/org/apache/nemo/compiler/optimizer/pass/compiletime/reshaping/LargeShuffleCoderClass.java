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
import java.nio.ByteBuffer;

final class LargeShuffleCoderClass implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(LargeShuffleCoderClass.class.getName());

  static final class LengthPaddingEncoderFactory implements EncoderFactory {

    final EncoderFactory valueEncoderFactory;

    LengthPaddingEncoderFactory(final EncoderFactory valueEncoderFactory) {
      this.valueEncoderFactory = valueEncoderFactory;
    }

    @Override
    public Encoder create(final OutputStream outputStream) throws IOException {
      final DirectByteArrayOutputStream byteOutputStream = new DirectByteArrayOutputStream();
      return new LengthPaddingEncoder(
        valueEncoderFactory.create(byteOutputStream), outputStream, byteOutputStream);
    }
  }

  static final class LengthPaddingDecoderFactory implements DecoderFactory {

    LengthPaddingDecoderFactory() {
    }

    @Override
    public Decoder create(InputStream inputStream) throws IOException {
      return new LengthPaddingDecoder(inputStream);
    }
  }

  static final class LengthPaddingEncoder<T> implements EncoderFactory.Encoder<T> {

    private final EncoderFactory.Encoder<T> valueEncoder;
    private final DirectByteArrayOutputStream bos;
    private final OutputStream outputStream;

    private LengthPaddingEncoder(final EncoderFactory.Encoder<T> valueEncoder,
                                 final OutputStream outputStream,
                                 final DirectByteArrayOutputStream bos) {
      this.valueEncoder = valueEncoder;
      this.bos = bos;
      this.outputStream = outputStream;
    }

    @Override
    public void encode(T element) throws IOException {
      // The value encoder will encode the value to the bos
      LOG.info("Encode");
      valueEncoder.encode(element);
      bos.close();

      int len = bos.getCount();
      //final ByteBuffer byteBuffer = ByteBuffer.allocate(4).putInt(len);
      //final byte[] lenByte = byteBuffer.array();
      //outputStream.write(lenByte);
      //bos.writeTo(outputStream);

      LOG.info("Encode length: {}, {}", len, element);
      final DataOutputStream dos = new DataOutputStream(outputStream);
      dos.writeInt(len);
      bos.writeTo(dos);

      //LOG.info("Encoded bytes: {}", byteArrayToHex(bos.toByteArray()));

      // close bos
    }

    @Override
    public String toString() {
      return "{LengthPaddingEncoder: " + valueEncoder + "}";
    }
  }

  static final class LengthPaddingDecoder implements DecoderFactory.Decoder {

    private final InputStream inputStream;
    private DirectByteArrayOutputStream byteArrayOutputStream;
    private boolean buffering = false;
    private int len;

    private LengthPaddingDecoder(final InputStream inputStream) {
      this.inputStream = inputStream;
    }

    @Override
    public byte[] decode() throws IOException {

      if (!buffering) {
        // this just returns byte array
        LOG.info("Start decode from inputStream: {}", inputStream);
        final DataInputStream dis = new DataInputStream(inputStream);
        len = dis.readInt();
        LOG.info("Decode len: {}", len);
        buffering = true;
        byteArrayOutputStream = new DirectByteArrayOutputStream(len);
      }

      // debug
      //final DirectByteArrayOutputStream byteOutputStream = new DirectByteArrayOutputStream(len);

      while (byteArrayOutputStream.getCount() < len) {
        int b = inputStream.read();

        if (b == -1) {
          throw new RuntimeException();
        }

        byteArrayOutputStream.write(b);
      }

      buffering = false;

      return byteArrayOutputStream.getBufDirectly();
      /*
      for (int cnt = 0; cnt < len; cnt++) {
        int b = inputStream.read();

        if (b == -1) {
          throw new RuntimeException("The byte length should be " + len + " , but " + byteOutputStream.getCount());
        }

        byteOutputStream.write(b);
      }

      byteOutputStream.close();

      LOG.info("Decoded byte length: {}, size: {}", byteOutputStream.getCount(), byteOutputStream.size());
      */

      /*
      if (byteOutputStream.getCount() != len) {
        throw new RuntimeException("The byte length should be " + len + " , but " + byteOutputStream.getCount());
      }

      final byte[] arr = new byte[len];
      System.arraycopy(byteOutputStream.getBufDirectly(), 0, arr, 0, len);
      */

      //return byteOutputStream.getBufDirectly();
    }
  }

  private static String byteArrayToHex(byte[] a) {
    StringBuilder sb = new StringBuilder();
    for(final byte b: a)
      sb.append(String.format("%02x ", b&0xff));
    return sb.toString();
  }
}
