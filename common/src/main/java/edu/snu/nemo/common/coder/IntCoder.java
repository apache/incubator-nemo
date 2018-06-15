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
package edu.snu.nemo.common.coder;

import java.io.*;

/**
 * A {@link Coder} which is used for an integer.
 */
public final class IntCoder implements Coder<Integer> {

  /**
   * Constructor.
   */
  public IntCoder() {
  }

  /**
   * Static initializer of the coder.
   */
  public static IntCoder of() {
    return new IntCoder();
  }

  @Override
  public void encode(final Integer value, final OutputStream outStream) throws IOException {
    final DataOutputStream dataOutputStream = new DataOutputStream(outStream);
    dataOutputStream.writeInt(value);
  }

  @Override
  public Integer decode(final InputStream inStream) throws IOException {
    // If the inStream is closed well in upper level, it is okay to not close this stream
    // because the DataInputStream itself will not contain any extra information.
    // (when we close this stream, the inStream will be closed together.)
    final DataInputStream dataInputStream = new DataInputStream(inStream);
    final int value = dataInputStream.readInt();
    return value;
  }
}
