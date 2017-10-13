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
package edu.snu.onyx.common.coder;

import edu.snu.onyx.compiler.frontend.beam.BeamElement;
import edu.snu.onyx.compiler.ir.Element;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * {@link Coder} from {@link org.apache.beam.sdk.coders.Coder}.
 *
 * @param <Data> data type.
 * @param <Key> key type.
 * @param <Value> value type.
 */
public final class BeamCoder<Data, Key, Value> implements Coder<Data, Key, Value> {
  private final org.apache.beam.sdk.coders.Coder<Data> beamCoder;

  public BeamCoder(final org.apache.beam.sdk.coders.Coder<Data> beamCoder) {
    this.beamCoder = beamCoder;
  }

  @Override
  public void encode(final Element<Data, Key, Value> value, final OutputStream outStream) {
    try {
      beamCoder.encode(value.getData(), outStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Element<Data, Key, Value> decode(final InputStream inStream) {
    try {
      return (Element<Data, Key, Value>)
          new BeamElement(beamCoder.decode(inStream));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return beamCoder.toString();
  }
}
