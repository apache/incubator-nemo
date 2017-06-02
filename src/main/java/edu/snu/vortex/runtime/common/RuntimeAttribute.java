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
// TODO #249: Clean up Attributes
public enum RuntimeAttribute {
  /**
   * Set of attributes applicable to {@link edu.snu.vortex.runtime.common.execplan.RuntimeVertex}.
   */

  /**
   * IRVertex container type attributes.
   */
  Default(Key.ContainerType),
  Transient(Key.ContainerType),
  Reserved(Key.ContainerType),
  Compute(Key.ContainerType),
  Storage(Key.ContainerType),


  /**
   * Block store attributes.
   * Local: The intermediate data is placed without serialization
   * and the receiver on the same machine task will take the data.
   *
   * Memory: The intermediate data is serialized and stored in sender task's memory,
   * until it is sent to the receiver task.
   * OOM occurs when the data size exceeds the memory capacity of the machine.
   *
   * File: The intermediate data is serialized and stored in sender task's local file system,
   * until it is are sent to the receiver task.
   * Data of size greater than the available disk space is lost.
   *
   * MemoryFile: The intermediate data is serialized and stored first in sender task's memory,
   * and spilled to the local file system if the data size exceeds the memory capacity.
   * Data of size greater than the available memory + disk space is lost.
   *
   * DistributedStorage: The intermediate data is serialized and stored in a distributed storage,
   * until it is taken by the receiver task.
   */
  Local(Key.BlockStore),
  Memory(Key.BlockStore),
  File(Key.BlockStore),
  MemoryFile(Key.BlockStore),
  DistributedStorage(Key.BlockStore),

  /**
   * Data transfer policy attributes.
   */
  Push(Key.PullOrPush),
  Pull(Key.PullOrPush),

  /**
   * IREdge partition type attributes.
   */
  Hash(Key.Partition),
  Range(Key.Partition),

  /**
   * IREdge type to specify edges that transfer SideInputs.
   */
  SideInput(Key.SideInput),

  /**
   * IREdge communication pattern attributes.
   */
  OneToOne(Key.CommPattern),
  Broadcast(Key.CommPattern),
  ScatterGather(Key.CommPattern),

  /**
   * SchedulingPolicy type attributes.
   */
  RoundRobin(Key.SchedulingPolicy),

  /**
   * Scheduler type attributes.
   */
  Batch(Key.Scheduler);

  /**
   * Runtime attribute keys.
   */
  public enum Key {
    ContainerType,
    BlockStore,
    PullOrPush,
    Partition,
    SideInput,
    CommPattern,
    SchedulingPolicy,
    Scheduler
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
