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
package edu.snu.vortex.compiler.ir;

/**
 * Attributes class.
 */
public enum Attributes {
  /**
   * Operator attributes.
   */
  Transient(Key.Placement),
  Reserved(Key.Placement),
  Compute(Key.Placement),

  /**
   * Edge partitioning attributes.
   */
  Hash(Key.EdgePartitioning),
  Range(Key.EdgePartitioning),

  /**
   * Edge attributes.
   */
  Memory(Key.EdgeChannel),
  TCPPipe(Key.EdgeChannel),
  File(Key.EdgeChannel),
  DistributedStorage(Key.EdgeChannel);

  /**
   * Attribute Keys.
   */
  public enum Key {
    Placement,
    EdgePartitioning,
    Parallelism,
    EdgeChannel,
  }

  private final Key key;

  Attributes(final Key key) {
    this.key = key;
  }

  public boolean hasKey(final Key k) {
    return key == k;
  }
}
