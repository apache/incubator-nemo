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
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.compiler.frontend.beam.SideInputElement;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * EncoderFactory for side inputs.
 */

public final class PushBackCoder extends Coder {
  private final Coder<Object> sideInputCoder;
  private final Coder<Object> mainInputCoder;

  private boolean startEncode = false;
  private boolean startDecode = false;

  public PushBackCoder(final Coder sideInputcoder,
                       final Coder mainInputCoder) {
    this.sideInputCoder = sideInputcoder;
    this.mainInputCoder = mainInputCoder;
  }


  @Override
  public void encode(Object value, OutputStream outStream) throws CoderException, IOException {
    if (!startEncode) {
      sideInputCoder.encode(value, outStream);
      startEncode = true;
    } else {
      mainInputCoder.encode(value, outStream);
    }
  }

  @Override
  public Object decode(final InputStream inStream) throws IOException {
    if (!startDecode) {
      startDecode = true;
      return sideInputCoder.decode(inStream);
    } else {
      return mainInputCoder.decode(inStream);
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
