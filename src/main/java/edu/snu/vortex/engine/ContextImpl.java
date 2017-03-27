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
package edu.snu.vortex.engine;

import edu.snu.vortex.compiler.ir.Transform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transform Context Implementation.
 */
public final class ContextImpl implements Transform.Context {
  private final Map<Transform, Object> sideInputs;

  ContextImpl() {
    this.sideInputs = new HashMap<>();
  }

  ContextImpl(final Map<Transform, Object> sideInputs) {
    this.sideInputs = sideInputs;
  }

  @Override
  public List<String> getSrcVertexIds() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getDstVertexIds() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<Transform, Object> getSideInputs() {
    return this.sideInputs;
  }
}
