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
package edu.snu.nemo.compiler.frontend.beam.coder;

import org.apache.beam.sdk.coders.AtomicCoder;

import java.io.*;

public final class IntArrayCoder extends AtomicCoder<int[]> {
  private IntArrayCoder() {
  }

  public static IntArrayCoder of() {
    return new IntArrayCoder();
  }

  @Override
  public void encode(final int[] ary, final OutputStream outStream) throws IOException {
    final DataOutputStream dataOutputStream = new DataOutputStream(outStream);
    dataOutputStream.writeInt(ary.length);
    for (int i : ary) {
      dataOutputStream.writeInt(i);
    }
  }

  @Override
  public int[] decode(final InputStream inStream) throws IOException {
    final DataInputStream dataInputStream = new DataInputStream(inStream);
    final int intArrayLen = dataInputStream.readInt();
    final int[] intArray = new int[intArrayLen];
    for (int i = 0; i < intArrayLen; i++) {
      intArray[i] = dataInputStream.readInt();
    }
    return intArray;
  }
}
