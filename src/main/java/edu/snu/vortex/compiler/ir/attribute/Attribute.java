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
  None(Key.Placement),
  Transient(Key.Placement),
  Reserved(Key.Placement),
  Compute(Key.Placement),

  /**
   * MetricCollectionBarrierVertex dynamic optimization type.
   */
  DataSkew(Key.DynamicOptimizationType),

  /**
   * IREdge channel data placement attributes.
   *
   * Memory: Intermediate data is stored in sender task's memory,
   * until it is sent to the receiver task.
   *
   * LocalFile: Intermediate data is serialized and stored in the local file system of the sender task's executor,,
   * until it is consumed by the receiver task.
   *
   * RemoteFile: Intermediate data is serialized and stored in a remote file system,
   * until it is consumed by the receiver task.
   */
  Memory(Key.ChannelDataPlacement),
  LocalFile(Key.ChannelDataPlacement),
  RemoteFile(Key.ChannelDataPlacement),

  /**
   * IREdge channel transfer policy attributes.
   */
  Pull(Key.ChannelTransferPolicy),
  Push(Key.ChannelTransferPolicy),

  /**
   * IREdge partitioning attributes.
   */
  Hash(Key.Partitioning),
  Range(Key.Partitioning),

  /**
   * IREdge communication pattern attributes.
   */
  OneToOne(Key.CommunicationPattern),
  Broadcast(Key.CommunicationPattern),
  ScatterGather(Key.CommunicationPattern),

  /**
   * IREdge type to specify edges that transfer SideInputs.
   */
  SideInput(Key.SideInput),

  /**
   * IREdge DataSizeMetricCollection attribute.
   */
  MetricCollection(Key.DataSizeMetricCollection),

  /**
   * IREdge WriteOptimization attributes.
   */
  IFileWrite(Key.WriteOptimization),

  /**
   * SchedulingPolicy type attributes.
   */
  RoundRobin(Key.SchedulingPolicy),

  /**
   * Scheduler type attributes.
   */
  Batch(Key.Scheduler);

  /**
   * Attribute Keys.
   */
  public enum Key {
    // IRVertex
    Placement,
    DynamicOptimizationType,

    // IREdge
    ChannelDataPlacement,
    ChannelTransferPolicy,
    Partitioning,
    CommunicationPattern,
    SideInput,
    DataSizeMetricCollection,
    WriteOptimization,

    // Scheduling
    SchedulingPolicy,
    Scheduler,
  }

  /**
   * Attribute keys that have Integer value.
   */
  public enum IntegerKey {
    // IRVertex
    Parallelism,
    StageId,
    ScheduleGroupIndex,
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
