/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.common.ir;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * ID manager.
 */
public final class IdManager {
  /**
   * Private constructor.
   */
  private IdManager() {
  }

  private static AtomicInteger vertexId = new AtomicInteger(1);
  private static AtomicInteger edgeId = new AtomicInteger(1);
  private static volatile boolean isDriver = false;

  /**
   * @return a new operator ID.
   */
  public static String newVertexId() {
    return "vertex" + (isDriver ? "-d" : "-") + vertexId.getAndIncrement();
  }
  /**
   * @return a new edge ID.
   */
  public static String newEdgeId() {
    return "edge" + (isDriver ? "-d" : "-") + edgeId.getAndIncrement();
  }

  /**
   * Set the realm of the loaded class as REEF driver.
   */
  public static void setInDriver() {
    isDriver = true;
  }
}
