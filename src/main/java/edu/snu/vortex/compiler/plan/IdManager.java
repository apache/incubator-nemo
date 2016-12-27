/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.vortex.compiler.plan;

import java.util.concurrent.atomic.AtomicInteger;

public class IdManager {
  private static AtomicInteger nodeId = new AtomicInteger(1);
  private static AtomicInteger edgeId = new AtomicInteger(1);

  public static String newNodeId() {
    return "node" + nodeId.getAndIncrement();
  }
  public static String newEdgeId() {
    return "edge" + edgeId.getAndIncrement();
  }
}
