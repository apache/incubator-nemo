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
package org.apache.nemo.compiler.frontend.beam.coder;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.compiler.frontend.beam.SideInputElement;

import java.io.*;

/**
 * EncoderFactory for side inputs.
 */
public final class SideInputCoder extends AtomicCoder<SideInputElement> {
  final WindowedValue.FullWindowedValueCoder windowedValueCoder;

  /**
   * Private constructor.
   */
  private SideInputCoder(final WindowedValue.FullWindowedValueCoder windowedValueCoder) {
    this.windowedValueCoder = windowedValueCoder;
  }

  /**
   * @return a new coder
   */
  public static SideInputCoder of(final WindowedValue.FullWindowedValueCoder windowedValueCoder) {
    return new SideInputCoder(windowedValueCoder);
  }

  @Override
  public void encode(final SideInputElement sideInputElement, final OutputStream outStream) throws IOException {
    final DataOutputStream dataOutputStream = new DataOutputStream(outStream);
    dataOutputStream.writeInt(sideInputElement.getViewIndex());
    windowedValueCoder.encode(sideInputElement.getData(), dataOutputStream);
  }

  @Override
  public SideInputElement decode(final InputStream inStream) throws IOException {
    final DataInputStream dataInputStream = new DataInputStream(inStream);
    final int index = dataInputStream.readInt();
    final WindowedValue windowedValue = windowedValueCoder.decode(inStream);
    return new SideInputElement(index, windowedValue);
  }
}
