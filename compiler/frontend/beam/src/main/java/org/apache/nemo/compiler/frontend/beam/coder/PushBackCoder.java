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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.common.Pair;
import org.apache.nemo.compiler.frontend.beam.SideInputElement;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * OffloadingEncoder for side inputs.
 */

public final class PushBackCoder extends Coder<Pair<WindowedValue<SideInputElement>, List<WindowedValue>>> {
  private final Coder<WindowedValue<SideInputElement>> sideInputCoder;
  private final Coder<WindowedValue> mainInputCoder;


  public PushBackCoder(final Coder<WindowedValue<SideInputElement>> sideInputcoder,
                       final Coder<WindowedValue> mainInputCoder) {
    this.sideInputCoder = sideInputcoder;
    this.mainInputCoder = mainInputCoder;
  }

  @Override
  public void encode(Pair<WindowedValue<SideInputElement>, List<WindowedValue>> value, OutputStream outStream) throws CoderException, IOException {
    // do nothing
    throw new RuntimeException("Unsupport!");
  }

  @Override
  public Pair<WindowedValue<SideInputElement>, List<WindowedValue>> decode(InputStream inStream) throws CoderException, IOException {

    final List<WindowedValue> mainInputs = new ArrayList<>();

    while (true) {
      final DataInputStream dis = new DataInputStream(inStream);
      final boolean isMain = dis.readBoolean();

      if (isMain) {
        final WindowedValue mainData = mainInputCoder.decode(inStream);
        mainInputs.add(mainData);
      } else {
        final WindowedValue<SideInputElement> sideData = sideInputCoder.decode(inStream);
        return Pair.of(sideData, mainInputs);
      }
    }
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(sideInputCoder, mainInputCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic(this, "Requires deterministic valueCoder", mainInputCoder);
  }
}
