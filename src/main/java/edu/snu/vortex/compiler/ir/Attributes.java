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
 * TODO #21: Refactor Attributes Class
 */
public final class Attributes {
  /**
   * Attribute Keys
   */
  public enum Key {
    Placement,
    EdgePartitioning,
    Parallelism,
    EdgeChannel,
  }

  /**
   * Attribute Vals
   */
  public interface Val {
  }

  public enum Placement implements Val {
    Transient,
    Reserved,
    Compute,
    Storage,
  }

  public enum EdgePartitioning implements Val {
    Hash,
    Range,
  }

  public enum EdgeChannel implements Val {
    Memory,
    TCPPipe,
    File,
    DistributedStorage,
  }
}
