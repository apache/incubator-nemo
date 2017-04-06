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
package edu.snu.vortex.runtime.common;

/**
 * Runtime attributes.
 */
public enum RuntimeAttribute {
  /**
   * Set of attributes applicable to {@link edu.snu.vortex.runtime.common.execplan.RuntimeVertex}.
   */

  /**
   * Vertex resource type attributes.
   */
  Transient(Key.ResourceType),
  Reserved(Key.ResourceType),
  Compute(Key.ResourceType),
  Storage(Key.ResourceType),


  /**
   * Channel data placement attributes.
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
   * until they are taken by the receiver task.
   */
  Local(Key.ChannelDataPlacement),
  Memory(Key.ChannelDataPlacement),
  File(Key.ChannelDataPlacement),
  DistributedStorage(Key.ChannelDataPlacement),

  /**
   * Channel transfer policy attributes.
   */
  Push(Key.ChannelTransferPolicy),
  Pull(Key.ChannelTransferPolicy),

  /**
   * Edge partition type attributes.
   */
  Hash(Key.Partition),
  Range(Key.Partition),

  /**
   * Edge type to specify edges that transfer SideInputs.
   */
  SideInput(Key.SideInput),

  /**
   * Edge communication pattern attributes.
   */
  OneToOne(Key.CommPattern),
  Broadcast(Key.CommPattern),
  ScatterGather(Key.CommPattern),

  /**
   * Scheduler type attributes.
   */
  SamplePolicy(Key.SchedulingPolicy),
  Batch(Key.SchedulingPolicy);

  /**
   * Runtime attribute keys.
   */
  public enum Key {
    ResourceType,
    ChannelDataPlacement,
    ChannelTransferPolicy,
    Partition,
    SideInput,
    CommPattern,
    SchedulingPolicy,
  }

  /**
   * Runtime attribute keys that have an integer as its value.
   */
  public enum IntegerKey {
    Parallelism,
  }

  private final RuntimeAttribute.Key key;

  RuntimeAttribute(final RuntimeAttribute.Key key) {
    this.key = key;
  }

  public boolean hasKey(final RuntimeAttribute.Key k) {
    return key == k;
  }

}
