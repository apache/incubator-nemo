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
package edu.snu.nemo.common;

import edu.snu.nemo.common.ir.vertex.transform.Transform;

import java.util.Map;
import java.util.Optional;

/**
 * Transform Context Implementation.
 */
public final class ContextImpl implements Transform.Context {
  private final Map sideInputs;
  private Optional<String> data;

  /**
   * Constructor of Context Implementation.
   * @param sideInputs side inputs.
   */
  public ContextImpl(final Map sideInputs) {
    this.sideInputs = sideInputs;
    this.data = Optional.empty();
  }

  @Override
  public Map getSideInputs() {
    return this.sideInputs;
  }

  @Override
  public void setSerializedData(final String serializedData) {
    this.data = Optional.of(serializedData);
  }

  @Override
  public Optional<String> getSerializedData() {
    return this.data;
  }
}
