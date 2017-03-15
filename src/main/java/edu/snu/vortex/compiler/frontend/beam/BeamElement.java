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
package edu.snu.vortex.compiler.frontend.beam;

import edu.snu.vortex.compiler.ir.Element;
import org.apache.beam.sdk.values.KV;

/**
 * Element implementation for Beam.
 * @param <Data> data type. It is casted as KV for Element's key-value getter methods.
 * @param <Key> key type.
 * @param <Value> value type.
 */
public final class BeamElement<Data, Key, Value> implements Element<Data, Key, Value> {
  private final Data data;

  public BeamElement(final Data d) {
    this.data = d;
  }

  @Override
  public Data getData() {
    return data;
  }

  @Override
  public Key getKey() {
    return ((KV<Key, Value>) data).getKey();
  }

  @Override
  public Value getValue() {
    return ((KV<Key, Value>) data).getValue();
  }

  @Override
  public String toString() {
    return data.toString();
  }
}
