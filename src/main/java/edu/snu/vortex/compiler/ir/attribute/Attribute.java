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
package edu.snu.vortex.compiler.ir.attribute;

/**
 * Attributes class.
 */
public enum Attribute {
  /**
   * IRVertex placement attributes.
   */
  Transient(Key.Placement),
  Reserved(Key.Placement),
  Compute(Key.Placement),
  Storage(Key.Placement),

  /**
   * IREdge partitioning attributes.
   */
  Hash(Key.Partitioning),
  Range(Key.Partitioning),

  /**
   * IREdge channel data placement attributes.
   * Local: Intermediate data are placed without serialization
   * and the receiver on the same machine task will take the data.
   *
   * Memory: Intermediate data are serialized and stored in sender task's memory,
   * until they are sent to the receiver task.
   *
   * File: Intermediate data are serialized and stored in local file,
   * until they are sent to the receiver task.
   *
   * DistributedStorage: Intermediate data are serialized and stored in a distributed storage,
   * until the receiver task takes them.
   */
  Local(Key.ChannelDataPlacement),
  Memory(Key.ChannelDataPlacement),
  File(Key.ChannelDataPlacement),
  DistributedStorage(Key.ChannelDataPlacement),

  /**
   * IREdge channel transfer policy attributes.
   */
  Pull(Key.ChannelTransferPolicy),
  Push(Key.ChannelTransferPolicy),

  /**
   * IREdge communication pattern attributes.
   */
  OneToOne(Key.CommunicationPattern),
  Broadcast(Key.CommunicationPattern),
  ScatterGather(Key.CommunicationPattern),

  /**
   * IREdge type to specify edges that transfer SideInputs.
   */
  SideInput(Key.SideInput);

  /**
   * Attribute Keys.
   */
  public enum Key {
    // IRVertex
    Placement,

    // IREdge
    Partitioning,
    ChannelDataPlacement,
    ChannelTransferPolicy,
    CommunicationPattern,
    SideInput,
  }

  /**
   * Attribute keys that have Integer value.
   */
  public enum IntegerKey {
    // IRVertex
    Parallelism,

    // IREdge
  }

  private final Key key;

  /**
   * Constructor for the Attribute class.
   * @param key Key of the attribute.
   */
  Attribute(final Key key) {
    this.key = key;
  }

  /**
   * Tells whether or not the Attribute has the given key.
   * @param k the given key.
   * @return whether or not the Attribute has the given key.
   */
  public boolean hasKey(final Key k) {
    return key == k;
  }
}
