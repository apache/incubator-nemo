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
package edu.snu.vortex.runtime.executor.data.partition;

import edu.snu.vortex.compiler.ir.Element;

/**
 * This class represents a {@link Partition} which is stored in local memory and not serialized.
 */
public final class MemoryPartition implements Partition {

  private final Iterable<Element> data;

  public MemoryPartition(final Iterable<Element> data) {
    this.data = data;
  }

  @Override
  public Iterable<Element> asIterable() {
    return data;
  }
}
