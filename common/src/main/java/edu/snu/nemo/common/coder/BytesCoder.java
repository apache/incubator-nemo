/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.nemo.common.coder;

import java.io.*;

/**
 * A {@link Coder} which is used for an array of bytes.
 */
public final class BytesCoder implements Coder<byte[]> {

  /**
   * Constructor.
   */
  public BytesCoder() {
  }

  @Override
  public void encode(final byte[] value, final OutputStream outStream) throws IOException {
    try (final DataOutputStream dataOutputStream = new DataOutputStream(outStream)) {
      dataOutputStream.writeInt(value.length); // Write the size of this byte array.
      dataOutputStream.write(value);
    }
  }

  @Override
  public byte[] decode(final InputStream inStream) throws IOException {
    // If the inStream is closed well in upper level, it is okay to not close this stream
    // because the DataInputStream itself will not contain any extra information.
    // (when we close this stream, the inStream will be closed together.)
    final DataInputStream dataInputStream = new DataInputStream(inStream);
    final int bytesToRead = dataInputStream.readInt();
    final byte[] bytes = new byte[bytesToRead]; // Read the size of this byte array.
    final int readBytes = dataInputStream.read(bytes);
    if (bytesToRead != readBytes) {
      throw new IOException("Have to read " + bytesToRead + " but read only " + readBytes + " bytes.");
    }
    return bytes;
  }
}
