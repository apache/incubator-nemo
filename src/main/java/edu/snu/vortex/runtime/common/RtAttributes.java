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

public final class RtAttributes {
  /**
   * Set of attributes applicable to {@link RtStage}.
   */
  public enum RtStageAttribute {PARALLELISM}

  /**
   * Set of attributes applicable to {@link RtOperator}.
   */
  public enum RtOpAttribute {PARTITION, RESOURCE_TYPE}

  /**
   * Set of values possible when a {@link RtOperator}'s attribute key is "PARTITION".
   */
  public enum Partition {HASH, RANGE}

  /**
   * Set of values possible when a {@link RtOperator}'s attribute key is "RESOURCE_TYPE".
   */
  public enum ResourceType {TRANSIENT, RESERVED, COMPUTE, STORAGE}

  /**
   * Set of attributes applicable to {@link RtOpLink}.
   */
  public enum RtOpLinkAttribute {CHANNEL, COMM_PATTERN}

  /**
   * Set of values possible when a {@link RtOpLink}'s attribute key is "CHANNEL".
   */
  public enum Channel {LOCAL_MEM, TCP, FILE, DISTR_STORAGE}

  /**
   * Set of values possible when a {@link RtOpLink}'s attribute key is "COMM_PATTERN".
   */
  public enum CommPattern {ONE_TO_ONE, BROADCAST, SCATTER_GATHER}
}
