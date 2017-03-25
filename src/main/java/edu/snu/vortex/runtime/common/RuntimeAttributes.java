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
public final class RuntimeAttributes {
  /**
   * Set of attributes applicable to {@link edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex}.
   */
  public enum RuntimeVertexAttribute { PARALLELISM, RESOURCE_TYPE }

  /**
   * Set of values possible when {@link RuntimeVertexAttribute} is "RESOURCE_TYPE".
   */
  public enum ResourceType { TRANSIENT, RESERVED, COMPUTE, STORAGE }

  /**
   * Set of attributes applicable to {@link edu.snu.vortex.runtime.common.plan.logical.RuntimeEdge}.
   */
  public enum RuntimeEdgeAttribute { CHANNEL, COMM_PATTERN, PARTITION }

  /**
   * Set of values possible when {@link RuntimeEdgeAttribute} is "CHANNEL".
   */
  public enum Channel { LOCAL_MEM, TCP, FILE, DISTR_STORAGE }

  /**
   * Set of values possible when {@link RuntimeEdgeAttribute} is "COMM_PATTERN".
   */
  public enum CommPattern { ONE_TO_ONE, BROADCAST, SCATTER_GATHER }

  /**
   * Set of values possible when {@link RuntimeEdgeAttribute} is "PARTITION".
   */
  public enum Partition { HASH, RANGE }
}
